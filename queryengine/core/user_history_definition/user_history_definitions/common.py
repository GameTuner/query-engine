# Copyright (c) 2024 AlgebraAI All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from queryengine.core.datasource.datasource import Column, DataType
from queryengine.core.user_history_definition.user_history_definition import UserHistoryDefinition, RegistrationColumn

COUNTRIES = ["Afghanistan", "Albania", "Algeria", "American Samoa", "Andorra", "Angola", "Anguilla", "Antigua and Barbuda", "Argentina", "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia, Plurinational State of", "Bonaire, Saint Eustatius and Saba", "Bosnia and Herzegovina", "Botswana", "Brazil", "British Indian Ocean Territory", "Brunei Darussalam", "Bulgaria", "Burkina Faso", "Burundi", "Cambodia", "Cameroon", "Canada", "Cape Verde", "Cayman Islands", "Central African Republic", "Chad", "Chile", "China", "Colombia", "Comoros", "Congo", "Congo, the Democratic Republic of the", "Cook Islands", "Costa Rica", "Croatia", "Cuba", "Curaçao", "Cyprus", "Czech Republic", "Côte d'Ivoire", "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Estonia", "Ethiopia", "Falkland Islands (Malvinas)", "Faroe Islands", "Fiji", "Finland", "France", "French Guiana", "French Polynesia", "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Gibraltar", "Greece", "Greenland", "Grenada", "Guadeloupe", "Guam", "Guatemala", "Guernsey", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Holy See (Vatican City State)", "Honduras", "Hong Kong", "Hungary", "Iceland", "India", "Indonesia", "Iran, Islamic Republic of", "Iraq", "Ireland", "Isle of Man", "Israel", "Italy", "Jamaica", "Japan", "Jersey", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Korea, Republic of", "Kuwait", "Kyrgyzstan", "Lao People's Democratic Republic", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libyan Arab Jamahiriya", "Liechtenstein", "Lithuania", "Luxembourg", "Macao", "Macedonia, the former Yugoslav Republic of", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Martinique", "Mauritania", "Mauritius", "Mayotte", "Mexico", "Micronesia, Federated States of", "Moldova, Republic of", "Monaco", "Mongolia", "Montenegro", "Montserrat", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands", "New Caledonia", "New Zealand", "Nicaragua", "Niger", "Nigeria", "Norfolk Island", "Northern Mariana Islands", "Norway", "Oman", "Pakistan", "Palau", "Palestinian Territory, Occupied", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Poland", "Portugal", "Puerto Rico", "Qatar", "Romania", "Russian Federation", "Rwanda", "Réunion", "Saint Barthélemy", "Saint Helena, Ascension and Tristan da Cunha", "Saint Kitts and Nevis", "Saint Lucia", "Saint Martin (French part)", "Saint Pierre and Miquelon", "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Sint Maarten (Dutch part)", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname", "Svalbard and Jan Mayen", "Swaziland", "Sweden", "Switzerland", "Syrian Arab Republic", "Taiwan, Province of China", "Tajikistan", "Tanzania, United Republic of", "Thailand", "Timor-Leste", "Togo", "Tokelau", "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Turks and Caicos Islands", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu", "Venezuela, Bolivarian Republic of", "Viet Nam", "Virgin Islands, British", "Virgin Islands, U.S.", "Wallis and Futuna", "Western Sahara", "Yemen", "Zambia", "Zimbabwe", "Åland Islands"]
PLATFORMS = ["android", "ios", "pc"]

USER_HISTORY_REGISTRATION_COLUMNS = [
    RegistrationColumn.from_column(Column('date_', DataType.date, hidden=True)),
    RegistrationColumn.from_column(Column('unique_id', DataType.string, can_group_by=False)),
    RegistrationColumn.from_column(Column('cohort_size', DataType.string, can_filter=False, can_group_by=False)),

    # registration
    RegistrationColumn.from_column(Column('registration_country', DataType.string, available_values=COUNTRIES)),
    RegistrationColumn.from_column(Column('last_login_country', DataType.string, available_values=COUNTRIES)),
    RegistrationColumn.from_column(Column('registration_platform', DataType.string, available_values=PLATFORMS)),
    RegistrationColumn.from_column(Column('last_login_platform', DataType.string, available_values=PLATFORMS)),
    RegistrationColumn.from_column(Column('registration_application_version', DataType.string)),
    RegistrationColumn.from_column(Column('last_login_application_version', DataType.string)),
    RegistrationColumn.from_column(Column('registration_date', DataType.date)),
    RegistrationColumn.from_column(Column('registration_week', DataType.date)),
    RegistrationColumn.from_column(Column('registration_month', DataType.date)),
    RegistrationColumn.from_column(Column('cohort_day', DataType.number)),

    # activity
    RegistrationColumn.from_column(Column('days_active_last_7_days', DataType.number)),
    RegistrationColumn.from_column(Column('days_active_last_28_days', DataType.number)),
    RegistrationColumn.from_column(Column('days_active', DataType.number)),
    RegistrationColumn.from_column(Column('dau', DataType.number, label='DAU', can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('dau_yesterday', DataType.number, label='DAU Yesterday', can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('wau', DataType.number, label='WAU', can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('mau', DataType.number, label='MAU', can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('mau_lost', DataType.number, label='MAU Lost', can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('mau_reactivated', DataType.number, label='MAU Reactivated', can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('last_login_day', DataType.date, label="Last Login Date")),
    RegistrationColumn.from_column(Column('previous_login_day', DataType.date, label="Previous Login Date")),
    RegistrationColumn.from_column(Column('sessions_count', DataType.number, label="Number of Sessions", can_group_by=False)),
    RegistrationColumn.from_column(Column('sessions_count_total', DataType.number, label="Total Number of Sessions", can_group_by=False)),
    RegistrationColumn.from_column(Column('playtime', DataType.number, can_group_by=False)),
    RegistrationColumn.from_column(Column('playtime_total', DataType.number, label="Total Playtime", can_group_by=False)),

    # revenue
    RegistrationColumn.from_column(Column('gross_revenue_usd', DataType.number, label='IAP Revenue Gross', can_group_by=False)),
    RegistrationColumn.from_column(Column('gross_revenue_usd_total', DataType.number, label='Total IAP Revenue Gross', can_group_by=False)),
    RegistrationColumn.from_column(Column('net_revenue_usd', DataType.number, label='IAP Revenue Net', can_group_by=False)),
    RegistrationColumn.from_column(Column('net_revenue_usd_total', DataType.number, label='Total IAP Revenue Net', can_group_by=False)),
    RegistrationColumn.from_column(Column('transactions_count', DataType.number, can_group_by=False)),
    RegistrationColumn.from_column(Column('transactions_count_total', DataType.number, label='Total Transactions Count', can_group_by=False)),
    RegistrationColumn.from_column(Column('is_payer', DataType.boolean)),
    RegistrationColumn.from_column(Column('is_repeated_payer', DataType.boolean)),
    RegistrationColumn.from_column(Column('new_payers', DataType.number, can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('total_payers', DataType.number, can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('daily_payers', DataType.number, can_filter=False, can_group_by=False)),
    RegistrationColumn.from_column(Column('days_since_last_purchase', DataType.number)),
]
USER_HISTORY_DEFINITION = UserHistoryDefinition(
    registration_columns={c.registration_table_column: c for c in USER_HISTORY_REGISTRATION_COLUMNS},
    external_table_columns={},
    total_columns={},
    computed_columns={},
)

APPSFLYER_COLUMNS = [
    Column('date_', DataType.date, hidden=True),
    Column('registration_date', DataType.date),
    Column('cohort_day', DataType.number),
    Column('media_source', DataType.string),
    Column('campaign', DataType.string),
    Column('campaign_id', DataType.string),
    Column('adset', DataType.string),
    Column('adset_id', DataType.string),
    Column('ad', DataType.string),
    Column('ad_id', DataType.string),
    Column('country_name', DataType.string),
    Column('platform', DataType.string),


    Column('cost', DataType.number, can_filter=False, can_group_by=False),
    Column('impressions', DataType.number, can_filter=False, can_group_by=False),
    Column('clicks', DataType.number, can_filter=False, can_group_by=False),
    Column('installs', DataType.number, can_filter=False, can_group_by=False),

    Column('iap_revenue_unique', DataType.number, can_filter=False, can_group_by=False),
    Column('iap_revenue_count', DataType.number, can_filter=False, can_group_by=False),
    Column('iap_revenue_count_total', DataType.number, can_filter=False, can_group_by=False),
    Column('iap_revenue_usd', DataType.number, can_filter=False, can_group_by=False),
    Column('iap_revenue_usd_total', DataType.number, can_filter=False, can_group_by=False),
    Column('iap_net_revenue_usd', DataType.number, can_filter=False, can_group_by=False),
    Column('iap_net_revenue_usd_total', DataType.number, can_filter=False, can_group_by=False),
    Column('ad_revenue_unique', DataType.number, can_filter=False, can_group_by=False),
    Column('ad_revenue_count', DataType.number, can_filter=False, can_group_by=False),
    Column('ad_revenue_count_total', DataType.number, can_filter=False, can_group_by=False),
    Column('ad_revenue_usd', DataType.number, can_filter=False, can_group_by=False),
    Column('ad_revenue_usd_total', DataType.number, can_filter=False, can_group_by=False),
    Column('unique_users', DataType.number, can_filter=False, can_group_by=False),
    Column('session_count', DataType.number, can_filter=False, can_group_by=False),
    Column('session_count_total', DataType.number, can_filter=False, can_group_by=False),
]
