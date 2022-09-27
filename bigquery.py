import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
from google.cloud import bigquery
from google.oauth2 import service_account
import logging

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Connected to BigQuery
credentials = service_account.Credentials.from_service_account_file(
    "./google-credentials.json"
)
project_id = "testsemplio"
client = bigquery.Client(credentials=credentials, project=project_id)

# Find the ward_code for the postcode from user input
def find_ward_code(your_postcode):
    query = """
            SELECT osward
            FROM `testsemplio.population.post`
            WHERE pcd = @pcd """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pcd", "STRING", your_postcode),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    find_ward_cod = query_job.result().to_dataframe().to_dict("records")
    if len(find_ward_cod) > 0:
        your_code = find_ward_cod[0]["osward"]
        return your_code
    else:
        return None


def predicted_population(your_code, your_year):
    query = """
            SELECT value, name_x, borough
            FROM `testsemplio.population.pop`
            WHERE code = @code and year = @year """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("code", "STRING", your_code),
            bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    predicted_population_dict = query_job.result().to_dataframe().to_dict("records")
    if len(predicted_population_dict) > 0:
        your_borough = predicted_population_dict["borough"]
        return predicted_population_dict, your_borough
    else:
        return None


def predicted_population_by_sex(your_code, your_year):
    # Find predicted male population for the user ward_code
    query = """
            SELECT value
            FROM `testsemplio.population.male`
            WHERE code = @code and year = @year"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("code", "STRING", your_code),
            bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    male_dict = query_job.result().to_dataframe().to_dict("records")[0]
    male_total = male_dict["value"]

    # Find predicted female population for the user ward_code
    query = """
            SELECT value
            FROM `testsemplio.population.female1`
            WHERE code = @code and year = @year"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("code", "STRING", your_code),
            bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    fem_dict = query_job.result().to_dataframe().to_dict("records")[0]
    female_total = fem_dict["value"]

    return male_total, female_total


def predicted_population_by_age(your_code, your_year):
    query = """
            SELECT age, sum(value) as value
            FROM `testsemplio.population.fin_age`
            where code = @code
            and year = @year
            group by age """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("code", "STRING", your_code),
            bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result().to_dataframe()
    sum_of_ages = result["value"].sum()
    print(sum_of_ages)
    child = round(
        100
        * (result.loc[(result["age"] >= 0) & (result["age"] < 13), "value"].sum())
        / sum_of_ages,
        2,
    )
    teens = round(
        100
        * (result.loc[(result["age"] >= 13) & (result["age"] < 20), "value"].sum())
        / sum_of_ages,
        2,
    )
    adults = round(
        100
        * (result.loc[(result["age"] >= 20) & (result["age"] < 40), "value"].sum())
        / sum_of_ages,
        2,
    )
    mid_age_adults = round(
        100
        * (result.loc[(result["age"] >= 40) & (result["age"] < 60), "value"].sum())
        / sum_of_ages,
        2,
    )
    seniors = round(
        100 * (result.loc[result["age"] >= 60, "value"].sum()) / sum_of_ages, 2
    )
    data = {
        "children": child,
        "teens": teens,
        "adults": adults,
        "mid_age_adults": mid_age_adults,
        "seniors": seniors,
    }
    predict_pop_by_age = pd.DataFrame(data=data, index=[0])

    return predict_pop_by_age


# Creating of Population pyramid
def plot_for_population_pyramid(your_code, your_year, your_postcode, your_borough):
    query = """
            SELECT male_table.age, male, female
            FROM (select age, sum(value) as male
            from `testsemplio.population.fin_age`
            where code = @code and year = @year and sex = 'male'
            group by age)
            as male_table
            join
            (SELECT age, female
            FROM (select age, sum(value) as female
            from `testsemplio.population.fin_age`
            where code = @code and year = @year and sex = 'female'
            group by age))
            as female_table
            on male_table.age = female_table.age"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("code", "STRING", your_code),
            bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
        ]
    )
    df1 = client.query(query, job_config=job_config).to_dataframe()
    male_baby = df1.loc[df1["age"] <= 10, "male"].sum()
    female_baby = df1.loc[df1["age"] <= 10, "female"].sum()
    male_teen = df1.loc[(df1["age"] >= 11) & (df1["age"] <= 17), "male"].sum()
    female_teen = df1.loc[(df1["age"] >= 11) & (df1["age"] <= 17), "female"].sum()
    male_young = df1.loc[(df1["age"] >= 18) & (df1["age"] <= 26), "male"].sum()
    female_young = df1.loc[(df1["age"] >= 18) & (df1["age"] <= 26), "female"].sum()
    male_adult = df1.loc[(df1["age"] >= 27) & (df1["age"] <= 37), "male"].sum()
    female_adult = df1.loc[(df1["age"] >= 27) & (df1["age"] <= 37), "female"].sum()
    male_mid_age = df1.loc[(df1["age"] >= 38) & (df1["age"] <= 50), "male"].sum()
    female_mid_age = df1.loc[(df1["age"] >= 38) & (df1["age"] <= 50), "female"].sum()
    male_grand_age = df1.loc[(df1["age"] >= 51) & (df1["age"] <= 65), "male"].sum()
    female_grand_age = df1.loc[(df1["age"] >= 51) & (df1["age"] <= 65), "female"].sum()
    male_old = df1.loc[(df1["age"] >= 66) & (df1["age"] < 80), "male"].sum()
    female_old = df1.loc[(df1["age"] >= 66) & (df1["age"] < 80), "female"].sum()
    male_very_old = df1.loc[df1["age"] >= 80, "male"].sum()
    female_very_old = df1.loc[df1["age"] >= 80, "female"].sum()

    df = pd.DataFrame(
        {
            "age": [
                "0-10",
                "11-17",
                "18-26",
                "27-37",
                "38-50",
                "51-65",
                "66-79",
                "80+",
            ],
            "male": [
                male_baby,
                male_teen,
                male_young,
                male_adult,
                male_mid_age,
                male_grand_age,
                male_old,
                male_very_old,
            ],
            "female": [
                female_baby,
                female_teen,
                female_young,
                female_adult,
                female_mid_age,
                female_grand_age,
                female_old,
                female_very_old,
            ],
        }
    )

    tick_lab = [
        "20000",
        "15000",
        "10000",
        "5000",
        "0",
        "5000",
        "10000",
        "15000",
        "20000",
    ]
    tick_val = [
        -20000,
        -15000,
        -10000,
        -5000,
        0,
        5000,
        10000,
        15000,
        20000,
    ]

    plt.xticks(tick_val, tick_lab)

    df = df.sort_values(["age"], ascending=False).reset_index(drop=True)
    df["male"] = df["male"] * -1

    bar_plot = sns.barplot(
        x=df["male"],
        y=df["age"],
        data=df,
        order=df["age"],
        color="darksalmon",
        edgecolor="w",
    )

    bar_plot = sns.barplot(
        x=df["female"],
        y=df["age"],
        data=df,
        order=df["age"],
        color="mediumseagreen",
        edgecolor="w",
    )

    for i in bar_plot.containers:
        dv = list(i.datavalues)
        dv = [str(int(abs(val))) for val in dv]
        bar_plot.bar_label(i, labels=dv, label_type="center")

    bar_plot.set_xlabel("Male/Female", fontsize=10)
    bar_plot.set_ylabel("Age", fontsize=10)
    bar_plot.set_title(
        f"Population Pyramid for {your_postcode} "
        f"in {your_borough}, predicted for {your_year}",
        fontdict={"fontsize": 10, "fontweight": "bold"},
    )
    plt.tight_layout()
    population_pyramid = io.BytesIO()
    plt.savefig(population_pyramid, format="png")
    population_pyramid.name = "plot.png"
    population_pyramid.seek(0)

    # Close the chart and clean information,
    # that used for building it
    plt.clf()
    plt.cla()
    plt.close()

    return population_pyramid


# Find the difference of the house prise in the Borough
def house_price_difference(your_borough):
    query = """
            SELECT ((may_2022-may_2021)*100/may_2021) as value
            FROM `testsemplio.population.h_p`
            where borough_name = @name"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("name", "STRING", your_borough),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result().to_dataframe().to_dict("records")[0]
    house_price = round(result["value"], 2)
    return house_price


# Find the median household income
# by the postcode from user input
def median_household_income(your_postcode):
    query = """
            SELECT round(income_lsoa, 1) as value
            FROM `testsemplio.population.economic_by_postcode`
            where postcode = @postcode and title = 'median'"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("postcode", "STRING", your_postcode),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result().to_dataframe().to_dict("records")[0]
    median_hh_income = round(result["value"], 2)

    return median_hh_income


# Find the household size by people that live in it for the ward_code
def household_size_f(predicted_population_dict):
    print(predicted_population_dict["name_x"])
    query = """
            SELECT category, sum(count) as value 
            FROM `testsemplio.population.hh_size` 
            where name = @name 
            group by(category)"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "name", "STRING", predicted_population_dict["name_x"]
            ),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    result = query_job.result().to_dataframe()

    household_size = result[:-1]

    try:
        household_size["value_percent"] = round(
            100 * household_size["value"] / sum(household_size["value"]), 2
        )

    except:
        raise Exception

    return household_size


def barchart_for_household_sizes(household_size):
    df = pd.DataFrame(
        {
            "hh_size": ["1", "2", "3", "4", "5", "6", "7", "8+"],
            "#_hh": [
                household_size["value"].iloc[0],
                household_size["value"].iloc[1],
                household_size["value"].iloc[2],
                household_size["value"].iloc[3],
                household_size["value"].iloc[4],
                household_size["value"].iloc[5],
                household_size["value"].iloc[6],
                household_size["value"].iloc[7],
            ],
        }
    )

    barplot = sns.barplot(x=df["hh_size"], y=df["#_hh"], palette="crest")

    plt.xticks(fontsize=8)
    for n in barplot.containers:
        barplot.bar_label(n, label_type="edge")

    barplot.set_ylabel("Number of households", fontsize=10)
    barplot.set_xlabel("Number of persons in household", fontsize=10)
    barplot.set_title(
        "Household sizes", fontdict={"fontsize": 15, "fontweight": "bold"}
    )
    plt.tight_layout()
    household_size_plot = io.BytesIO()
    plt.savefig(household_size_plot, format="png")
    household_size_plot.name = "hhsize.png"
    household_size_plot.seek(0)

    # Close the chart and clean information,
    # that used for building it
    plt.clf()
    plt.cla()
    plt.close()

    return household_size_plot


# Find the work and workless households for the Borough
def working_and_workless_households(your_borough):
    query = """
        SELECT work_hh_value, mix_hh_value, workless_hh_value
        FROM `testsemplio.population.work_hh`
        where borough_name = @name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("name", "STRING", your_borough),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    work_and_workless_dict = query_job.result().to_dataframe().to_dict("records")[0]

    work_and_workless_dict["work_hh_value"] *= 1000
    work_and_workless_dict["mix_hh_value"] *= 1000
    work_and_workless_dict["workless_hh_value"] *= 1000

    sum_work = sum(work_and_workless_dict.values())

    work_and_workless_dict["percent_of_working_hh"] = round(
        100 * work_and_workless_dict["work_hh_value"] / sum_work, 2
    )
    work_and_workless_dict["percent_of_mixed_hh"] = round(
        100 * work_and_workless_dict["mix_hh_value"] / sum_work, 2
    )
    work_and_workless_dict["percent_of_workless_hh"] = round(
        100 * work_and_workless_dict["workless_hh_value"] / sum_work, 2
    )

    return work_and_workless_dict


# Creating barchart for the work and workless households
def barchart_for_work_and_workless_hh(work_and_workless_dict):
    df = pd.DataFrame(
        {
            "work": ["working", "mixed", "workless"],
            "number": [
                work_and_workless_dict["work_hh_value"],
                work_and_workless_dict["mix_hh_value"],
                work_and_workless_dict["workless_hh_value"],
            ],
        }
    )
    plot_bar = sns.barplot(y=df["work"], x=df["number"], palette="pastel")

    for i in plot_bar.containers:
        plot_bar.bar_label(i, label_type="center")

    plot_bar.set_ylabel("types of households", fontsize=10)
    plot_bar.set_xlabel("number of households", fontsize=10)
    plot_bar.set_title(
        "Types of households by work criteria",
        fontdict={"fontsize": 15, "fontweight": "bold"},
    )
    plt.yticks(rotation=30)
    plt.tight_layout()
    work_and_workless_hh_plot = io.BytesIO()
    plt.savefig(work_and_workless_hh_plot, format="png")
    work_and_workless_hh_plot.name = "work.png"
    work_and_workless_hh_plot.seek(0)

    # Close the chart and clean information,
    # that used for building it
    plt.clf()
    plt.cla()
    plt.close()

    return work_and_workless_hh_plot


# Find the category of tenure in the ward_code
def tenure_categories_f(predicted_population_dict):
    query = """
            SELECT category, sum(value) as value
            FROM `testsemplio.population.tenure1`
            where name = @name
            group by category"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "name", "STRING", predicted_population_dict["name_x"]
            ),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result().to_dataframe()
    tenure_categories = result.drop([0, 1, 5, 8]).reset_index(drop=True)
    tenure_categories["value_percent"] = round(
        100 * tenure_categories["value"] / sum(tenure_categories["value"]), 2
    )

    return tenure_categories


# Find socialgrade by the postcode from user input
def socialgrade(your_postcode):
    query = """
            SELECT title,
            round(socialgrade_oa *100/sum(socialgrade_oa)over(), 0)
            as socialgrade_oa,
            round(socialgrade_ward*100/sum(socialgrade_ward) over(), 0)
            as socialgrade_ward,
            FROM ( SELECT title, socialgrade_oa, socialgrade_ward
                FROM `testsemplio.population.economic_by_postcode`
                where postcode = @postcode
                and socialgrade_oa is not null
                and title!= 'ab_bucket'
                and title!= 'de_bucket'
                and title != 'total')"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("postcode", "STRING", your_postcode),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    socialgrade_table = query_job.result().to_dataframe()
    cols = ["socialgrade_oa", "socialgrade_ward"]
    socialgrade_table[cols] = socialgrade_table[cols].astype(int)

    return socialgrade_table


# Fin qualification of population by the postcode
def qualification_of_population(your_postcode):
    query = """
            SELECT title,
            round(qualification_oa *100/sum(qualification_oa)over(), 0)
            as qualification_oa,
            round(qualification_ward*100/sum(qualification_ward) over(), 0)
            as qualification_ward,
            FROM ( SELECT title, qualification_oa, qualification_ward
                FROM `testsemplio.population.economic_by_postcode`
                where postcode = @postcode
                and qualification_oa is not null
                and title !='level_4_bucket'
                and title !='schoolchildren_and_students_over_18'
                and title !='total'
                and title !='students_over_18_to_74_unemployed'
                and title !='schoolchildren_and_students_16_to_17'
                and title !='students_over_18_to_74_employed'
                and title != 'students_over_18_to_74_inactive'
                and title !='no_qualification_bucket')"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("postcode", "STRING", your_postcode),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    qualification_table = query_job.result().to_dataframe()
    cols = ["qualification_oa", "qualification_ward"]
    qualification_table[cols] = qualification_table[cols].astype(int)

    return qualification_table
