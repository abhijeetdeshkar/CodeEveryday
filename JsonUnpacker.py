import numpy as np
import pandas as pd

mongo_doc_all = {"_id":399504018,"wsc":{"fgn":"","isin":"CH1123378890","cusip":"","_file_id":3958,"currency":"USD","_file_dt":"2022-06-30T00:15:18Z"},"instrument":{"debt":{"bond_form":{"bond_form_type":"3"},"convertible_instruments":{"convert_details":{"convert_period":{"convert_period_schedule":{"convert_terms":"2","convert_date":"2022-08-17"},"convert_feature_type":"6","_start_date":"2022-08-17","_end_date":"2022-08-17","convert_timing":"7","issuer_same_as_underlying":"false","convert_initiator":"3"},"convert_code":"4","convert_option_type":"2","conditional_conversion_ind":"false"}},"bond_secured_by":{"secured_by_type":"12"},"bond_currency":{"currency_type":"12","currency_type_inclusion":"2","currency_code":"USD"},"coupon_payment_feature":{"coupon_type":"8","unconfirmed_indicator":"true","stated_first_coupon_date":"2021-11-17","coupon_date_rules":[{"rule_code":"6","offset_num":{"_offset_unit_desc":"Day of Month","#text":"17","_offset_unit":"3"},"order_num":"1"},{"rule_code":"5","order_num":"2"}],"interest_basis":"12","payment_frequency":"3","contingent_interest_ind":"false"},"fixed_income":{"debt_rank_type":"0","other_tax_exempt_indicator":"false","principal_factorT1":"1.00000000","protect_principal_against_deflation":"false","guaranteed_type":"3","principal_factor":"1.00000000","stated_first_coupon_date":"2021-11-17","offering_type":"7","contingent_interest_indicator":"false","last_accrual_date":"2022-05-17","record_date_rules":"3","last_coupon_period_type":"2","principal_factorT4":"1.00000000","original_interest_payment_frequency":"5","deferred_interest_type":"3","compound_interest_indicator":"false","agency_discount_note_ind_indicator":"false","coupon_summary":"Fixed Rate - Unconfirmed","next_coupon_payment_date":"2022-08-17","strippable_indicator":"false","record_date_holiday_rules":"1","dtc_indicator":"false","tips_indicator":"false","interest_payment_frequency":"5","principal_factorT5":"1.00000000","coupon_type":"2","coupon_inflation_protected":"false","defeasance_indicator":"false","principal_factorT2":"1.00000000","daycount_basis_type":"12","debt_type":"25","tender_exchange_offer_ind":"false","accrued_interest_indicator":"false","default_indicator":"false","principal_factorT3":"1.00000000","pik_indicator":"false","protect_coupon_against_deflation":"false","first_coupon_date":"2021-11-17","last_day_of_month_payment_indicator":"false","convertible_indicator":"true","oid_indicator":"false","interest_payment_date_code":"1","maturity_date":"2022-08-17"},"bond_status":{"status":"1"}},"global_information":{"instrument_details":{"maturity_details":{"effective_maturity_date":"2022-08-17","bond_maturity_type":"6","interest_at_maturity_indicator":"false"},"eusd_details":{"eusd_type":"5"},"denomination_amounts":{"denom_increment_amount_secondary":"1000.0000000","min_denom_amount":"1000.000000","denom_increment_amount":"1000","available_denom":"1000.0000000"}},"country_information":{"instrument_country_information":{"country_code":"CH"}}},"_id":399504018,"master_information":{"instrument_source":{"derived_content_status_code":"2","content_status_code":"2","source_material_code":"2"},"instrument_comments":{"external_comment":"."},"instrument_xref":{"xref":[{"_type":"ISIN","#text":"CH1123378890","_type_id":"2"},{"_type":"CINS","#text":"H0R93ZUE1","_type_id":"5"},{"_type":"Valoren","#text":"112337889","_type_id":"11"},{"_type":"Bloomberg Global Id","_id_bb_sec_num":"BAER 0 08/17/22 0006","_exch_code":"NOT LISTED","_ticker":"BAER","_security_typ":"EURO-DOLLAR","_market_sector":"Corp","#text":"BBG012358965","_security_typ2":"Corp","_type_id":"20"}]},"organization_master":{"organization_type":"67","_id":"8101755","org_country_code":"CH","bond_ticker":"BAER","primary_name_abbreviated":"BANK JULIUS BAER & CO","organization_xref":{"xref":[{"_type":"Issuer Code","#text":"BAMCM","_type_id":"25","_entity_level":"issuer"},{"_type":"CUSIP-6","#text":"H0R93Z","_type_id":"26","_entity_level":"issuer"}]},"primary_name":"Bank Julius Baer& Co","organization_status":"6"},"instrument_master":{"primary_currency_code":"USD","instrument_status":"1","issue_price_type":"2","eval_quotation_basis":"1","primary_name_abbreviated":"RVCV 17/08/2022 USD1000","child_issue_ind":"false","instrument_type":"3","primary_name":"REV CONV 17/08/2022 USD","apex_asset_type":"1","overallotment_indicator":"false","registration_type":"7","unit_indicator":"false","creation_date":"2021-08-10","federal_tax_status":"3"},"market_master":{"market":{"quotation_basis":"2","country_of_quotation":"CH","_id":"399561771","_primary":"true","xref":{"_type":"IDII","#text":"3C28ZQ4","_type_id":"4","_status":"Live"}}}},"_file_id":3958}}
mongo_doc_sample = mongo_doc = {'_id': 399504018, 'wsc': {'cusip': '', '_file_id': 3958}, 'instrument': {'_id': 399504018,
                                                                                      'master_information': {
                                                                                          'instrument_xref': {'xref': [{
                                                                                              '_type': 'Bloomberg Global Id',
                                                                                              '_id_bb_sec_num': 'BAER 0 08/17/22 0006',
                                                                                              '_exch_code': 'NOT LISTED',
                                                                                              '_ticker': 'BAER',
                                                                                              '_security_typ': 'EURO-DOLLAR',
                                                                                              '_market_sector': 'Corp',
                                                                                              '#text': 'BBG012358965',
                                                                                              '_security_typ2': 'Corp',
                                                                                              '_type_id': '20'}]},
                                                                                          'instrument_master': {
                                                                                              'primary_name': 'REV CONV 17/08/2022 USD'}}}}

class Unpacker:
    """
    Class to flatten nested mongo documents to single level key-value mappings with nested keys represented using dot
    notation.

    """
    list_based_columns = list()
    failed_ids = list()

    def __init__(self, mongo_doc):
        self.mongo_doc = mongo_doc
        self.result = dict()
        self.unpack_nest_of_dict(self.mongo_doc)

    def unpack_nest_of_dict(self, data: dict, calling_key=""):
        for k, v in data.items():
            if isinstance(v, dict):
                key = f"{calling_key}{k}."
                self.unpack_nest_of_dict(v, calling_key=key)
            elif isinstance(k, str) and (isinstance(v, str) or isinstance(v, int)):
                flattened_element = f"{calling_key}{k}"
                self.result[flattened_element] = str(v)
            elif isinstance(v, list):
                flattened_element = f"{calling_key}{k}"
                self.list_based_columns.append(flattened_element)
                try:
                    flattened_data = pd.DataFrame(data=v)
                    flattened_data.replace(np.nan, "", inplace=True)
                    flattened_data = flattened_data.to_dict(orient="index")[0]
                    self.result.update(flattened_data)
                except:
                    self.failed_ids.append(self.result.get("_id", "NA"))

    @property
    def unpacked(self):
        return self.result

    @property
    def get_list_based_columns(self):
        return self.list_based_columns
