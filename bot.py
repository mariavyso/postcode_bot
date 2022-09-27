from telegram import (
    ReplyKeyboardMarkup,
    Update,
    ReplyKeyboardRemove,
)
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder
from telegram.ext import (
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
)
import logging
from bigquery import (
    find_ward_code,
    predicted_population,
    predicted_population_by_sex,
    predicted_population_by_age,
    plot_for_population_pyramid,
    house_price_difference,
    median_household_income,
    household_size_f,
    barchart_for_household_sizes,
    working_and_workless_households,
    barchart_for_work_and_workless_hh,
    tenure_categories_f,
    socialgrade,
    qualification_of_population,
)

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

CONTEXT = None
your_ward = None
your_year = None
CHOOSING, TYPING_REPLY, GO_FIND, DONE = range(4)

# Define keyboards
year_keyboard = [
    ["2019", "2020"],
    ["2021", "2022"],
]
markup = ReplyKeyboardMarkup(year_keyboard, one_time_keyboard=True)
y_n_keyboard = [
    ["yes", "no"],
]
markup2 = ReplyKeyboardMarkup(y_n_keyboard, one_time_keyboard=True)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    await update.message.reply_text(
        "Hi! Write your postcode in the London Area, please:",
    )
    return CHOOSING


async def regular_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Ask the user for info about the selected predefined choice."""
    text = update.message.text
    context.user_data["choice"] = text

    await update.message.reply_text(
        "Thanks. Now choose the year you want the information about:",
        reply_markup=markup,
    )

    return TYPING_REPLY


async def received_information(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Store info provided by user and ask for the next category."""
    user_data = context.user_data
    text = update.message.text
    category = user_data["choice"]
    user_data[category] = text
    del user_data["choice"]

    await update.message.reply_text(
        f"So you want to know about : {user_data}", reply_markup=markup2
    )

    return GO_FIND


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:

    # Log the error before we do anything else,
    # so we can see it even if something breaks.
    logger.error(msg="Exception while handling an update:", exc_info=context.error)

    # Send the message
    await update.message.reply_text(
        f"I'm dead. Write to Masha about me!\n"
        f"And let's try again with different postcode",
    )
    return await restart(update, context)


# Take user input and return everything nice and clean
def work_with_user_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_data = context.user_data
    your_postcode_input = list(user_data.items())[0][0]
    your_postcode = your_postcode_input.upper().replace(" ", "")
    your_year = list(user_data.items())[0][1]

    return your_postcode, your_year


async def finder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:

    # Find user's input(postcode, year)
    your_postcode, your_year = work_with_user_input(update, context)
    your_code = find_ward_code(your_postcode)
    if your_code == None:
        logger.error(msg="Exception while handling an update:", exc_info=context.error)
        # Send the message
        await update.message.reply_text(
            f"I cant find the ward code for your postcode, "
            f"it seems that your postcode is incorrect or there is no such postcode! \n"
            f"Let's try again with different postcode",
        )
        return await restart(update, context)

    # Find population
    predicted_population_dict, your_borough = predicted_population(your_code, your_year)
    if predicted_population_dict == None:
        logger.error(msg="Exception while handling an update:", exc_info=context.error)
        # Send the message
        await update.message.reply_text(
            f"Something went wrong, I cant find the Borough for your postcode!\n"
            f"Let's try again with different postcode",
        )
        return await restart(update, context)

    # Send messages with population info
    await update.message.reply_text(
        f"*Here are what I found for "
        f"{predicted_population_dict['name_x']} and {your_year}:*\n \n",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    await update.message.reply_text(
        f"Total population for {your_year} is: *{predicted_population_dict['value']}*;\n",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    # Find predicted population by sex
    male_total, female_total = predicted_population_by_sex(your_code, your_year)
    # Send messages with population distributed by sex
    await update.message.reply_text(
        f"Predicted male population for {your_year} is: *{male_total}*;\n",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    await update.message.reply_text(
        f"Predicted female population for {your_year} is: *{female_total}*;\n",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    # Find the distribution of population by age groups
    predict_pop_by_age = predicted_population_by_age(your_code, your_year)

    # Send the message with distribution of population by age groups
    await update.message.reply_text(
        f"Predicted number of: \n"
        f"- children (0-12): {predict_pop_by_age.iloc[0]['children']} %;\n"
        f"- teens (13-19): {predict_pop_by_age.iloc[0]['teens']} %;\n"
        f"- adults (20-39): {predict_pop_by_age.iloc[0]['adults']} %;\n"
        f"- mid-age adults (40-59): {predict_pop_by_age.iloc[0]['mid_age_adults']} %;\n"
        f"- seniors (60+): {predict_pop_by_age.iloc[0]['seniors']} %;\n \n"
    )

    # Create a plot with population pyramid
    population_pyramid = plot_for_population_pyramid(
        your_code, your_year, your_postcode, your_borough
    )
    # Send the plot to the user
    await context.bot.send_photo(
        chat_id=update.effective_chat.id, photo=population_pyramid
    )

    # Find the difference btw 2021 and 2022 house prices in user's borough
    house_price = house_price_difference(your_borough)
    # Send the info to the user
    await update.message.reply_text(
        f"<b>The following data is for 2021-2022:</b> \n\n\n"
        f"The difference of house price in "
        f"{your_borough} from may 2021 to may 2022: "
        f"<b>{house_price} % </b>;\n \n",
        parse_mode=ParseMode.HTML,
    )

    # Find median household income
    median_hh_income = median_household_income(your_postcode)
    # Send the message to the user
    await update.message.reply_text(
        f"Median household income for your postcode: "
        f"<b>£{median_hh_income}</b>; \n\n\n",
        parse_mode=ParseMode.HTML,
    )

    # Find the size of the households in the area
    household_size = household_size_f(predicted_population_dict)
    if household_size.empty == True:
        logger.error(msg="Exception while handling an update:", exc_info=context.error)
        # Send the message
        await update.message.reply_text(
            f"Something went wrong, I cant find the Borough for your postcode!\n"
            f"Let's try again with different postcode",
        )
        return await restart(update, context)
    # Send the message with that information
    await update.message.reply_text(
        f"The number of households with: \n"
        f"- 1 person : {household_size['value_percent'].iloc[0]} %;\n"
        f"- 2 people : {household_size['value_percent'].iloc[1]} %;\n"
        f"- 3 people : {household_size['value_percent'].iloc[2]} %;\n"
        f"- 4 people : {household_size['value_percent'].iloc[3]} %;\n"
        f"- 5 people : {household_size['value_percent'].iloc[4]} %;\n"
        f"- 6 people : {household_size['value_percent'].iloc[5]} %;\n"
        f"- 7 people : {household_size['value_percent'].iloc[6]} %;\n"
        f"- 8+ people : {household_size['value_percent'].iloc[7]} %;\n \n"
    )

    # Create a plot with visualizations of household sizes
    household_size_plot = barchart_for_household_sizes(household_size)
    # Send the plot to the user
    await context.bot.send_photo(
        chat_id=update.effective_chat.id, photo=household_size_plot
    )

    # Find work and workless households
    work_and_workless_dict = working_and_workless_households(your_borough)
    # Send the message to the user
    await update.message.reply_text(
        f"<b>Working households in {your_borough}:</b>\n"
        f"The number of the working households: {work_and_workless_dict['percent_of_working_hh']} %;\n"
        f"The number of the mixed (working+workless) households: "
        f"{work_and_workless_dict['percent_of_mixed_hh']} %;\n"
        f"The number of the workless households: "
        f"{work_and_workless_dict['percent_of_workless_hh']} %;\n\n"
        f"<b>*</b> 1 Household includes at least one person "
        f"aged 16 to 64\n\n",
        parse_mode=ParseMode.HTML,
    )

    # Create the plot of work and workless households
    work_and_workless_hh_plot = barchart_for_work_and_workless_hh(
        work_and_workless_dict
    )
    # Send the plot to the user
    await context.bot.send_photo(
        chat_id=update.effective_chat.id, photo=work_and_workless_hh_plot
    )

    # Find the tenure categories
    tenure_categories = tenure_categories_f(predicted_population_dict)
    # send the message
    await update.message.reply_text(
        f"<b>The number of people, who lived in:</b>\n"
        f"- owned outright residence: "
        f"{tenure_categories['value_percent'].iloc[0]} %;\n"
        f"- owned with a mortgage or loan residence: "
        f"{tenure_categories['value_percent'].iloc[1]} %;\n"
        f"- part owned and part rented residence: "
        f"{tenure_categories['value_percent'].iloc[2]} %;\n"
        f"- social rented from local authority residence: "
        f"{tenure_categories['value_percent'].iloc[3]} %;\n"
        f"- other social rented residence: {tenure_categories['value_percent'].iloc[4]} %;\n"
        f"- rented from private landlord or agency residence: "
        f"{tenure_categories['value_percent'].iloc[5]} %;\n"
        f"- private rented as an employer of a "
        f"household member residence: {tenure_categories['value_percent'].iloc[6]} %;\n"
        f"- private rented as a relative or friend "
        f"of household member residence: {tenure_categories['value_percent'].iloc[7]} %;\n"
        f"- other private rented residence: {tenure_categories['value_percent'].iloc[8]} %;\n"
        f"- rent free residence: {tenure_categories['value_percent'].iloc[9]} %;\n\n",
        parse_mode=ParseMode.HTML,
    )

    # Find socialgrade
    socialgrade_table = socialgrade(your_postcode)
    # Send the message
    await update.message.reply_text(
        f"<b>Social grade for this area: </b>\n"
        f"Higher and intermidiate managerial, administrative personal: "
        f"{socialgrade_table['socialgrade_oa'].iloc[2]} %;\n"
        f"Supervisory or clerical and junior managerial, "
        f"administrative or professional: {socialgrade_table['socialgrade_oa'].iloc[1]} %;\n"
        f"Skilled manual workers: {socialgrade_table['socialgrade_oa'].iloc[3]} %;\n"
        f"Semi and unskilled manual workers, "
        f"casual or lowest grade workers, pensioners and others "
        f"who depend on the state for their income: "
        f"{socialgrade_table['socialgrade_oa'].iloc[0]} %;\n\n",
        parse_mode=ParseMode.HTML,
    )

    await update.message.reply_text(
        f"<b>Social grade for this ward:</b> \n"
        f"Higher and intermidiate managerial, "
        f"administrative personal: {socialgrade_table['socialgrade_ward'].iloc[2]} %;\n"
        f"Supervisory or clerical and junior managerial, "
        f"administrative or professional: {socialgrade_table['socialgrade_ward'].iloc[1]} %;\n"
        f"Skilled manual workers: {socialgrade_table['socialgrade_ward'].iloc[3]} %;\n"
        f"Semi and unskilled manual workers, "
        f"casual or lowest grade workers, pensioners and others "
        f"who depend on the state for their income: "
        f"{socialgrade_table['socialgrade_ward'].iloc[0]} %;\n\n\n",
        parse_mode=ParseMode.HTML,
    )

    # Find qualification
    qualification_table = qualification_of_population(your_postcode)
    # Send the message
    await update.message.reply_text(
        f"<b>Qualification level in the area. "
        f"Percentage of people with:</b>\n"
        f"Degree level or above: {qualification_table['qualification_oa'].iloc[4]} %;\n"
        f"2+ A-levels or equivalent: "
        f"{qualification_table['qualification_oa'].iloc[3]} %;\n"
        f"Apprenticeship: Professional education: "
        f"{qualification_table['qualification_oa'].iloc[5]} %;\n"
        f"5+ GCSEs or equivalent: {qualification_table['qualification_oa'].iloc[2]} %;\n"
        f"1-4 GCSEs or equivalent: {qualification_table['qualification_oa'].iloc[6]} %;\n"
        f"No academic or professional qualifications: "
        f"{qualification_table['qualification_oa'].iloc[0]} %;\n"
        f"Other Qualification: Vocational / "
        f"Work-related Qualifications, Foreign Qualifications "
        f"/ Qualificationsgained outside the UK: "
        f"{qualification_table['qualification_oa'].iloc[1]} %;\n\n",
        parse_mode=ParseMode.HTML,
    )

    await update.message.reply_text(
        f"<b>Qualification level in the ward. "
        f"Percentage of people with:</b>\n"
        f"Degree level or above: {qualification_table['qualification_ward'].iloc[4]} %;\n"
        f"2+ A-levels or equivalent: "
        f"{qualification_table['qualification_ward'].iloc[3]} %;\n"
        f"Apprenticeship: Professional education: "
        f"{qualification_table['qualification_ward'].iloc[5]} %;\n"
        f"5+ GCSEs or equivalent: "
        f"{qualification_table['qualification_ward'].iloc[2]} %;\n"
        f"1-4 GCSEs or equivalent: {qualification_table['qualification_ward'].iloc[6]} %;\n"
        f"No academic or professional qualifications: "
        f"{qualification_table['qualification_ward'].iloc[0]} %;\n"
        f"Other Qualification: Vocational / "
        f"Work-related Qualifications, Foreign Qualifications "
        f"/ Qualificationsgained outside the UK: "
        f"{qualification_table['qualification_ward'].iloc[1]} %;\n\n\n",
        parse_mode=ParseMode.HTML,
    )

    await update.message.reply_text(
        f"Do you want to start again?\n",
        reply_markup=markup2,
        parse_mode=ParseMode.HTML,
    )

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
    # Create the Application and pass it bot's token.
    application = (
        ApplicationBuilder()
        .token("5496114181:AAFwGVZFxdNhFtP2oLI69kfdi1wgZLm1Bo8")
        .build()
    )

    # Add conversation handler with the states
    # CHOOSING, TYPING_CHOICE and TYPING_REPLY
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
