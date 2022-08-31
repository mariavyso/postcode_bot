from telegram import (
    ReplyKeyboardMarkup,
    Update,
    ReplyKeyboardRemove,
)
from telegram.ext import ApplicationBuilder
from telegram.ext import (
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
)
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
from telegram.constants import ParseMode
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
from io import BytesIO
import prettytable as pt



# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

credentials = service_account.Credentials.from_service_account_file(
    "./google-credentials.json"
)

project_id = "durable-sky-359421"
client = bigquery.Client(credentials=credentials, project=project_id)


CONTEXT = None

your_ward = None
your_year = None

CHOOSING, TYPING_REPLY, GO_FIND, DONE = range(4)


another_keyboard = [
    ["2019", "2020"],
    ["2021", "2022"],
]

markup2 = ReplyKeyboardMarkup(another_keyboard, one_time_keyboard=True)

keyboard = [
    ["yes", "no"],
]

markup3 = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    await update.message.reply_text(
        "Hi! Write your postcode without spaces(like this: NW110WS), please:",
    )
    return CHOOSING


async def regular_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Ask the user for info about the selected predefined choice."""
    text = update.message.text
    print(text)
    context.user_data["choice"] = text

    await update.message.reply_text(
        "Thanks. Now choose the year you want the information about:",
        reply_markup=markup2,
    )

    return TYPING_REPLY


async def received_information(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Store info provided by user and ask for the next category."""
    user_data = context.user_data
    print(user_data)
    text = update.message.text
    category = user_data["choice"]
    user_data[category] = text
    del user_data["choice"]

    await update.message.reply_text(
        f"So you want to know about : {user_data}", reply_markup=markup3
    )

    return GO_FIND

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a telegram message to notify the developer."""
    # Log the error before we do anything else, so we can see it even if something breaks.
    logger.error(msg="Exception while handling an update:", exc_info=context.error)

    # Finally, send the message
    await update.message.reply_text(f"I'm dead. Write to Masha about me!\n"
        f"And let's try again with different postcode",
        )
    return await restart(update, context)



async def finder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_data = context.user_data
    your_postcode1 = list(user_data.items())[0][0]
    your_postcode = your_postcode1.upper().replace(' ','')
    your_year = list(user_data.items())[0][1]
    print(your_postcode1, your_postcode, your_year)

    query = """
            SELECT osward
            FROM `durable-sky-359421.london_data.postcodes`
            WHERE pcd = @pcd """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pcd", "STRING", your_postcode),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()
    rows = list(result)
    print(rows)

    if rows == []:
        print ('here!!!!!')
        await update.message.reply_text(f"Something wrong with your postcode, I can't find ward code for it. Please, send the postcode you used to Masha!\n"
        f"And let's try again with different postcode")
        
        return await restart(update, context)
  

        



    else:
        your_code = rows[0][0]
        query = """
                SELECT value, name_x, borough
                FROM `durable-sky-359421.london_data.population`
                WHERE code = @code and year = @year """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("code", "STRING", your_code),
                bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
            ]
        )
        query_job = client.query(query, job_config=job_config)
        result = query_job.result()
        rows = list(result)
        your_ward = rows[0][1]
        pop_total = rows[0][0]
        your_borough = rows[0][2]
        if your_borough == [] or your_ward ==[]:
            await update.message.reply_text(
            f"Something wrong with your postcode, I can't find the borough for it. Please, send the postcode you used to Masha!\n"
            f"And let's try again with different postcode"
            )
            return await restart(update, context)
        else:
            query = """
                    SELECT value
                    FROM `durable-sky-359421.london_data.male`
                    WHERE code = @code and year = @year"""

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("code", "STRING", your_code),
                    bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
                ]
            )
            query_job = client.query(query, job_config=job_config)
            result = query_job.result()
            rows = list(result)
            male_total = rows[0][0]

            query = """
                    SELECT value
                    FROM `durable-sky-359421.london_data.female`
                    WHERE code = @code and year = @year"""

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("code", "STRING", your_code),
                    bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
                ]
            )

            query_job = client.query(query, job_config=job_config)
            result = query_job.result()
            rows = list(result)
            female_total = rows[0][0]

            query = """
                    SELECT category, sum(count) as value 
                    FROM `durable-sky-359421.london_data.hh_size` 
                    where name = @name 
                    group by(category)"""

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("name", "STRING", your_ward),
                ]
            )

            query_job = client.query(query, job_config=job_config)
            result = query_job.result()
            rows = list(result)
            print(rows)
            if rows == []:
                await update.message.reply_text(
                f"Something wrong with your postcode, I can't find the borough for it. Please, send the postcode you used to Masha!\n"
                f"And let's try again with different postcode"
                )
                return await restart(update, context)
            else:
                one_p_h = rows[0][1]
                two_p_h = rows[1][1]
                three_p_h = rows[2][1]
                four_p_h = rows[3][1]
                five_p_h = rows[4][1]
                six_p_h = rows[5][1]
                seven_p_h = rows[6][1]
                ei_p_h = rows[7][1]
                sum_hh = (
                    one_p_h
                    + two_p_h
                    + three_p_h
                    + four_p_h
                    + five_p_h
                    + six_p_h
                    + seven_p_h
                    + ei_p_h
                )
                fin_one_p_h = round(100 * one_p_h / sum_hh, 2)
                fin_two_p_h = round(100 * two_p_h / sum_hh, 2)
                fin_three_p_h = round(100 * three_p_h / sum_hh, 2)
                fin_four_p_h = round(100 * four_p_h / sum_hh, 2)
                fin_five_p_h = round(100 * five_p_h / sum_hh, 2)
                fin_six_p_h = round(100 * six_p_h / sum_hh, 2)
                fin_seven_p_h = round(100 * seven_p_h / sum_hh, 2)
                fin_ei_p_h = round(100 * ei_p_h / sum_hh, 2)

                #barchart for hh size
                df = pd.DataFrame({'hh_size': ["1","2","3","4","5","6","7","8+"], '#_hh':[one_p_h, two_p_h, three_p_h, four_p_h, five_p_h, six_p_h, seven_p_h, ei_p_h]})

                barplot = sns.barplot(x=df['hh_size'], y = df['#_hh'], palette = "crest")


                plt.xticks(fontsize = 8)
                for n in barplot.containers:
                    barplot.bar_label(n,label_type='edge')

                barplot.set_ylabel("Number of households", fontsize = 10)
                barplot.set_xlabel("Number of persons in household", fontsize = 10)
                barplot.set_title("Household sizes", fontdict= { 'fontsize': 15, 'fontweight':'bold'})
                plt.tight_layout()
                hh_size_plot = io.BytesIO()
                plt.savefig(hh_size_plot , format='png')
                hh_size_plot.name = 'hhsize.png'
                hh_size_plot.seek(0)
                plt.clf()
                plt.cla()
                plt.close()

                # number of children
                query = """
                        SELECT sum(value) 
                        FROM `durable-sky-359421.london_data.age` 
                        where code = @code and year = @year and age >=0 and age <13 """

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("code", "STRING", your_code),
                        bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                children = rows[0][0]

                # number of teens
                query = """
                        SELECT sum(value) 
                        FROM `durable-sky-359421.london_data.age`  
                        where code = @code and year = @year and age >=13 and age <20 """

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("code", "STRING", your_code),
                        bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                teens = rows[0][0]

                # number of adults
                query = """
                        SELECT sum(value) 
                        FROM `durable-sky-359421.london_data.age` 
                        where code = @code and year = @year and age >=20 and age <40 """

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("code", "STRING", your_code),
                        bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                adults = rows[0][0]

                # number of mid_age_adults
                query = """
                        SELECT sum(value) 
                        FROM `durable-sky-359421.london_data.age` 
                        where code = @code and year = @year and age >=40 and age <60 """

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("code", "STRING", your_code),
                        bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                mid_age_adults = rows[0][0]

                # number of seniors
                query = """
                        SELECT sum(value) 
                        FROM `durable-sky-359421.london_data.age`  
                        where code = @code and year = @year and age >=60"""

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("code", "STRING", your_code),
                        bigquery.ScalarQueryParameter("year", "INTEGER", your_year),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                seniors = rows[0][0]

                sum_ages = children + teens + adults + mid_age_adults + seniors
                f_children = round(100 * children / sum_ages, 2)
                f_teens = round(100 * teens / sum_ages, 2)
                f_adults = round(100 * adults / sum_ages, 2)
                f_mid_age_adults = round(100 * mid_age_adults / sum_ages, 2)
                f_seniors = round(100 * seniors / sum_ages, 2)

                #print the population pyramid

                query = """
                        SELECT male_table.age, male, female 
                        FROM (select age, sum(value) as male
                        from `durable-sky-359421.london_data.age` 
                        where code = @code and year = @year and sex = 'male'
                        group by age) 
                        as male_table
                        join
                        (SELECT age, female
                        FROM (select age, sum(value) as female
                        from `durable-sky-359421.london_data.age` 
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
                print(df1)

                male_baby = df1.loc[df1['age'] <= 10, 'male'].sum()
                female_baby = df1.loc[df1['age'] <= 10, 'female'].sum()
                male_teen = df1.loc[(df1['age']>= 11) & (df1['age']<= 17), 'male'].sum()
                female_teen = df1.loc[(df1['age']>= 11) & (df1['age']<= 17), 'female'].sum()
                male_young = df1.loc[(df1['age']>= 18) & (df1['age']<= 26), 'male'].sum()
                female_young = df1.loc[(df1['age']>= 18) & (df1['age']<= 26), 'female'].sum()
                male_adult =  df1.loc[(df1['age']>= 27) & (df1['age']<= 37), 'male'].sum()
                female_adult =  df1.loc[(df1['age']>= 27) & (df1['age']<= 37), 'female'].sum()
                male_mid_age = df1.loc[(df1['age']>= 38) & (df1['age']<= 50), 'male'].sum()
                female_mid_age = df1.loc[(df1['age']>= 38) & (df1['age']<= 50), 'female'].sum()
                male_grand_age = df1.loc[(df1['age']>= 51) & (df1['age']<= 65), 'male'].sum()
                female_grand_age = df1.loc[(df1['age']>= 51) & (df1['age']<= 65), 'female'].sum()
                male_old = df1.loc[(df1['age']>= 66) & (df1['age']< 80), 'male'].sum()
                female_old = df1.loc[(df1['age']>= 66) & (df1['age']< 80), 'female'].sum()
                male_very_old = df1.loc[df1['age']>= 80, 'male'].sum()
                female_very_old = df1.loc[df1['age']>= 80, 'female'].sum()

                df = pd.DataFrame({'age': ['0-10','11-17','18-26','27-37','38-50','51-65','66-79','80+'], 
                    'male': [male_baby, male_teen, male_young, male_adult, male_mid_age, male_grand_age, male_old, male_very_old], 
                    'female': [female_baby, female_teen, female_young, female_adult, female_mid_age, female_grand_age, female_old, female_very_old]})

                tick_lab = ['20000', '15000', '10000','5000','0', '5000', '10000', '15000', '20000']
                tick_val = [-20000, -15000, -10000, -5000, 0, 5000, 10000, 15000, 20000]

                plt.xticks(tick_val, tick_lab)


                df = df.sort_values(['age'], ascending=False).reset_index(drop=True)
                df['male'] = df['male'] * -1

                bar_plot = sns.barplot(x=df['male'], y=df['age'], data=df, order=df['age'], color='darksalmon', edgecolor = 'w')

                bar_plot = sns.barplot(x=df['female'], y=df['age'], data=df, order=df['age'], color = 'mediumseagreen',  edgecolor = 'w')

                for i in bar_plot.containers:
                    dv = list(i.datavalues)
                    print(dv)
                    dv = [str(int(abs(val))) for val in dv]
                    bar_plot.bar_label(i, labels=dv, label_type='center')

                bar_plot.set_xlabel("Male/Female", fontsize =10)
                bar_plot.set_ylabel("Age", fontsize = 10)
                bar_plot.set_title(f"Population Pyramid for {your_postcode} in {your_borough}, predicted for {your_year}", fontdict= { 'fontsize': 10, 'fontweight':'bold'})
                plt.tight_layout()
                pop_pyramid = io.BytesIO()
                plt.savefig(pop_pyramid , format='png')
                pop_pyramid.name = 'plot.png'
                pop_pyramid.seek(0)
                plt.clf()
                plt.cla()
                plt.close()



                # house price

                query = """
                        SELECT ((may_2022-may_2021)*100/may_2021)  
                        FROM `durable-sky-359421.london_data.house_price`  
                        where borough_name = @name"""

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("name", "STRING", your_borough),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                house_price_21_22 = rows[0][0]
                h_p = round(house_price_21_22, 2)

                # category of rent

                query = """
                        SELECT category, sum(value)  
                        FROM `durable-sky-359421.london_data.tenure`  
                        where name = @name
                        group by category"""

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("name", "STRING", your_ward),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                print(rows)
                own_out = rows[2][1]
                own_mort = rows[3][1]
                own_rent = rows[4][1]
                soc_rent_gov = rows[6][1]
                soc_rent_other = rows[7][1]
                priv_rent = rows[9][1]
                priv_rent_emp = rows[10][1]
                priv_rent_rel = rows[11][1]
                priv_rent_other = rows[12][1]
                rent_free = rows[13][1]
                sum_rent = (
                    own_out
                    + own_mort
                    + own_rent
                    + soc_rent_gov
                    + soc_rent_other
                    + priv_rent
                    + priv_rent_emp
                    + priv_rent_rel
                    + priv_rent_other
                    + rent_free
                )
                f_own_out = round(100 * own_out / sum_rent, 2)
                f_own_mort = round(100 * own_mort / sum_rent, 2)
                f_own_rent = round(100 * own_rent / sum_rent, 2)
                f_soc_rent_gov = round(100 * soc_rent_gov / sum_rent, 2)
                f_soc_rent_other = round(100 * soc_rent_other / sum_rent, 2)
                f_priv_rent = round(100 * priv_rent / sum_rent, 2)
                f_priv_rent_emp = round(100 * priv_rent_emp / sum_rent, 2)
                f_priv_rent_rel = round(100 * priv_rent_rel / sum_rent, 2)
                f_priv_rent_other = round(100 * priv_rent_other / sum_rent, 2)
                f_rent_free = round(100 * rent_free / sum_rent, 2)

                #workless hh by borough
                query = """
                        SELECT work_hh_value, mix_hh_value, workless_hh_value  
                        FROM `durable-sky-359421.london_data.work_hh_2019` 
                        where borough_name = @name"""

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("name", "STRING", your_borough),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                work_hh_value = rows[0][0]*1000
                mix_hh_value = rows[0][1]*1000
                workless_hh_value = rows[0][2]*1000
                sum_work = work_hh_value + mix_hh_value + workless_hh_value
                f_work_hh = round(100*work_hh_value/sum_work, 2)
                f_mix_hh = round(100*mix_hh_value/sum_work, 2)
                f_workless_hh = round(100*workless_hh_value/sum_work, 2)

                #barchart for working hh
                df = pd.DataFrame({'work':['working', 'mixed', 'workless'], 'number':[work_hh_value, mix_hh_value, workless_hh_value]})
                plot_bar = sns.barplot(y = df['work'], x = df['number'], palette= 'pastel')

                for i in plot_bar.containers:
                    plot_bar.bar_label(i,label_type='center')

                plot_bar.set_ylabel("types of households", fontsize = 10)
                plot_bar.set_xlabel("number of households", fontsize = 10)
                plot_bar.set_title("Types of households by work criteria", fontdict= { 'fontsize': 15, 'fontweight':'bold'})
                plt.yticks(rotation = 30)
                plt.tight_layout()
                work_hh = io.BytesIO()
                plt.savefig(work_hh, format='png')
                work_hh.name = 'work.png'
                work_hh.seek(0)
                plt.clf()
                plt.cla()
                plt.close()



                #median hh income 
                query = """
                        SELECT round(income_lsoa, 1)  
                        FROM `durable-sky-359421.london_data.econom_by_postcode`  
                        where postcode = @postcode and title = 'median'"""

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("postcode", "STRING", your_postcode),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                median_hh_income = rows[0][0]


                #socialgrade in your postcode
                query = """
                        SELECT title, 
                        round(socialgrade_oa *100/sum(socialgrade_oa)over(), 0) as socialgrade_oa,
                        round(socialgrade_ward*100/sum(socialgrade_ward) over(), 0) as socialgrade_ward, 
                        FROM ( SELECT title, socialgrade_oa, socialgrade_ward 
                            FROM `durable-sky-359421.london_data.econom_by_postcode` 
                            where postcode = @postcode and socialgrade_oa is not null and title!= 'ab_bucket' and title!= 'de_bucket' and title != 'total')"""

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("postcode", "STRING", your_postcode),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                c2_socialgrade_oa = rows[0][1]
                c1_socialgrade_oa = rows[1][1]
                ab_socialgrade_oa = rows[2][1]
                de_socialgrade_oa = rows[3][1]

                c2_socialgrade_ward = rows[0][2]
                c1_socialgrade_ward = rows[1][2]
                ab_socialgrade_ward = rows[2][2]
                de_socialgrade_ward = rows[3][2]


                #qualification
                query = """
                        SELECT title, 
                        round(qualification_oa *100/sum(qualification_oa)over(), 0) as qualification_oa,
                        round(qualification_ward*100/sum(qualification_ward) over(), 0) as qualification_ward,
                        FROM ( SELECT title, qualification_oa, qualification_ward 
                            FROM `durable-sky-359421.london_data.econom_by_postcode`
                            where postcode = @postcode and qualification_oa is not null and title !='level_4_bucket' and title !='schoolchildren_and_students_over_18' and title !='total'
                            and title !='students_over_18_to_74_unemployed' and title !='schoolchildren_and_students_16_to_17' and title !='students_over_18_to_74_employed' and title != 'students_over_18_to_74_inactive' 
                            and title !='no_qualification_bucket')"""

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("postcode", "STRING", your_postcode),
                    ]
                )
                query_job = client.query(query, job_config=job_config)
                result = query_job.result()
                rows = list(result)
                level2_qualification_oa = rows[0][1]
                level1_qualification_oa = rows[1][1]
                other_qualification_oa = rows[2][1]
                level3_qualification_oa = rows[3][1]
                level4_qualification_oa = rows[4][1]
                level_appr_qualification_oa = rows[5][1]
                no_qualification_oa = rows[6][1]

                level2_qualification_ward = rows[0][2]
                level1_qualification_ward = rows[1][2]
                other_qualification_ward = rows[2][2]
                level3_qualification_ward = rows[3][2]
                level4_qualification_ward = rows[4][2]
                level_appr_qualification_ward = rows[5][2]
                no_qualification_ward = rows[6][2]


                await update.message.reply_text(
                    f"*__Here are what I found for {your_ward} and {your_year}:__*\n \n",
                    parse_mode=ParseMode.MARKDOWN_V2)

                await update.message.reply_text(
                    f"Total population for {your_year} is: *{pop_total}*;\n",
                    parse_mode=ParseMode.MARKDOWN_V2)

                await update.message.reply_text(
                    f"Predicted male population for {your_year} is: *{male_total}*;\n",
                    parse_mode=ParseMode.MARKDOWN_V2)

                await update.message.reply_text(
                    f"Predicted female population for  {your_year} is: *{female_total}*;\n",
                    parse_mode=ParseMode.MARKDOWN_V2)
                                
                await update.message.reply_text(
                    f"Predicted number of: \n"
                    f"- children (0-12): {f_children} %;\n"
                    f"- teens (13-19): {f_teens} %;\n"
                    f"- adults (20-39): {f_adults} %;\n"
                    f"- mid-age adults (40-59): {f_mid_age_adults} %;\n"
                    f"- seniors (60+): {f_seniors} %;\n \n")

                await context.bot.send_photo(chat_id=update.effective_chat.id, photo = pop_pyramid)

                await update.message.reply_text(
                    f"*__The following data is for 2021\-2022:__* \n\n\n",
                    parse_mode=ParseMode.MARKDOWN_V2)
                
                await update.message.reply_text(
                    f"The difference of house price here from may 2021 to may 2022: <b>{h_p} % </b>;\n \n",
                    parse_mode=ParseMode.HTML)

                await update.message.reply_text(
                    f'Median household income for this postcode: <b>£{median_hh_income}</b>; \n\n\n',
                    parse_mode=ParseMode.HTML)

                await update.message.reply_text(    
                    f"The number of households with: \n"
                    f"- 1 person : {fin_one_p_h} %;\n"
                    f"- 2 people : {fin_two_p_h} %;\n"
                    f"- 3 people : {fin_three_p_h} %;\n"
                    f"- 4 people : {fin_four_p_h} %;\n"
                    f"- 5 people : {fin_five_p_h} %;\n"
                    f"- 6 people : {fin_six_p_h} %;\n"
                    f"- 7 people : {fin_seven_p_h} %;\n"
                    f"- 8+ people : {fin_ei_p_h} %;\n \n")

                await context.bot.send_photo(chat_id=update.effective_chat.id, photo = hh_size_plot)


                await update.message.reply_text(
                    f"<b>Working households in {your_borough}:</b>\n"
                    f"The number of the working households: {f_work_hh} %;\n"
                    f"The number of the mixed (working+workless) households: {f_mix_hh} %;\n"
                    f"The number of the workless households: {f_workless_hh} %;\n\n"
                    f"<b>*</b> 1 Household includes at least one person aged 16 to 64\n\n",
                    parse_mode=ParseMode.HTML)

                await context.bot.send_photo(chat_id=update.effective_chat.id, photo = work_hh)

                await update.message.reply_text(
                    f"<b>The number of people, who lived in:</b>\n"   
                    f"- owned outright residence: {f_own_out} %;\n"
                    f"- owned with a mortgage or loan residence: {f_own_mort} %;\n"
                    f"- part owned and part rented residence: {f_own_rent} %;\n"
                    f"- social rented from local authority residence: {f_soc_rent_gov} %;\n"
                    f"- other social rented residence: {f_soc_rent_other} %;\n"
                    f"- rented from private landlord or agency residence: {f_priv_rent} %;\n"
                    f"- private rented as an employer of a household member residence: {f_priv_rent_emp} %;\n"
                    f"- private rented as a relative or friend of household member residence: {f_priv_rent_rel} %;\n"
                    f"- other private rented residence: {f_priv_rent_other} %;\n"
                    f"- rent free residence: {f_rent_free} %;\n\n",
                    parse_mode=ParseMode.HTML)
                    
                await update.message.reply_text(    
                    f'<b>Social grade for this area: </b>\n'
                    f'Higher and intermidiate managerial, administrative personal: {ab_socialgrade_oa} %;\n'
                    f'Supervisory or clerical and junior managerial, administrative or professional: {c1_socialgrade_oa} %;\n'
                    f'Skilled manual workers: {c2_socialgrade_oa} %;\n'
                    f'Semi and unskilled manual workers, casual or lowest grade workers, pensioners and others who depend on the state for their income: {de_socialgrade_oa} %;\n\n',
                    parse_mode=ParseMode.HTML)


                await update.message.reply_text( 
                    f'<b>Social grade for this ward:</b> \n'
                    f'Higher and intermidiate managerial, administrative personal: {ab_socialgrade_ward} %;\n'
                    f'Supervisory or clerical and junior managerial, administrative or professional: {c1_socialgrade_ward} %;\n'
                    f'Skilled manual workers: {c2_socialgrade_ward} %;\n'
                    f'Semi and unskilled manual workers, casual or lowest grade workers, pensioners and others who depend on the state for their income: {de_socialgrade_ward} %;\n\n\n',
                    parse_mode=ParseMode.HTML)


                await update.message.reply_text( 
                    f'<b>Qualification level in the area. Percentage of people with:</b>\n'
                    f'Degree level or above: {level4_qualification_oa} %;\n'
                    f'2+ A-levels or equivalent: {level3_qualification_oa} %;\n'
                    f'Apprenticeship: Professional education: {level_appr_qualification_oa} %;\n'
                    f'5+ GCSEs or equivalent: {level2_qualification_oa} %;\n'
                    f'1-4 GCSEs or equivalent: {level1_qualification_oa} %;\n'
                    f'No academic or professional qualifications: {no_qualification_oa} %;\n'
                    f'Other Qualification: Vocational / Work-related Qualifications, Foreign Qualifications / Qualificationsgained outside the UK: {other_qualification_oa} %;\n\n',
                    parse_mode=ParseMode.HTML)

                await update.message.reply_text(
                    f'<b>Qualification level in the ward. Percentage of people with:</b>\n'
                    f'Degree level or above: {level4_qualification_ward} %;\n'
                    f'2+ A-levels or equivalent: {level3_qualification_ward} %;\n'
                    f'Apprenticeship: Professional education: {level_appr_qualification_ward} %;\n'
                    f'5+ GCSEs or equivalent: {level2_qualification_ward} %;\n'
                    f'1-4 GCSEs or equivalent: {level1_qualification_ward} %;\n'
                    f'No academic or professional qualifications: {no_qualification_ward} %;\n'
                    f'Other Qualification: Vocational / Work-related Qualifications, Foreign Qualifications / Qualificationsgained outside the UK: {other_qualification_ward} %;\n\n\n',
                    parse_mode=ParseMode.HTML)
                

                await update.message.reply_text(
                    f"Do you want to start again?\n", 
                    reply_markup=markup3,
                    parse_mode=ParseMode.HTML)

                return DONE


async def restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Display the gathered info and end the conversation."""
    user_data = context.user_data
    print(user_data)
    if "choice" in user_data:
        del user_data["choice"]

    await update.message.reply_text(
        f"Press /start!",
        reply_markup=ReplyKeyboardRemove(),
    )

    user_data.clear()
    return ConversationHandler.END


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Display the gathered info and end the conversation."""
    user_data = context.user_data
    print(user_data)
    if "choice" in user_data:
        del user_data["choice"]

    await update.message.reply_text(
        f"Until the next time!",
        reply_markup=ReplyKeyboardRemove(),
    )

    user_data.clear()
    return ConversationHandler.END


def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = (
        ApplicationBuilder()
        .token("5496114181:AAFwGVZFxdNhFtP2oLI69kfdi1wgZLm1Bo8")
        .build()
    )

    # Add conversation handler with the states CHOOSING, TYPING_CHOICE and TYPING_REPLY
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CHOOSING: [
                MessageHandler(
                    filters.TEXT,
                    regular_choice,
                )
            ],
            TYPING_REPLY: [
                MessageHandler(
                    filters.TEXT & ~(filters.COMMAND | filters.Regex("^Done$")),
                    received_information,
                )
            ],
            GO_FIND: [
                MessageHandler(
                    filters.Regex("^yes$"),
                    finder,
                ),
                MessageHandler(filters.Regex("^no$"), done),
            ],
            DONE: [
                MessageHandler(
                    filters.Regex("^yes$"),
                    restart,
                ), 
                MessageHandler(filters.Regex("^no$"), done),
            ],
        },
        fallbacks=[MessageHandler(filters.Regex("^Done$"), done)],
    )

    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)

    # Run the bot until the user presses Ctrl-C
    application.run_polling()


if __name__ == "__main__":
    main()
