# import pathlib
# from typing import Final
#
# from euets.data.iep import PathIep
# from euets.data.iep.utils import Layout
# from euets.utils.io import read_xml
#
#
# def load_waste_transfer(
#     layout: Layout = "wide",
#     # sanitise: bool = True,
#     balance_panel: bool = False,
#     # deduplicate: bool = True,
#     case_sensitive_id: bool = False,
#     aggregate_by_source: bool = True,
# ) -> pd.DataFrame:
#     data = read_xml(
#         fn=pathlib.Path(PathIep, "2h_OffsiteWasteTransfer.xml"),
#         dtype={
#             "fileId_EPRTR_LCP": "int64",
#             "OffsiteWasteTransferId": "int64",
#             "Facility_INSPIRE_ID": "string",
#             "reportingYear": "int64",
#             "wasteClassificationCode": "category",
#             "wasteTreatmentCode": "category",
#             "wasteTreatmentName": "category",
#             "totalWasteQuantityTNE": "float64",
#             "methodCode": "category",
#             "methodName": "category",
#             "furtherDetails": "string",
#             "confidentialityReasonCode": "category",
#             "confidentialityReasonName": "category",
#             "transboundaryIndicator": "boolean",
#             "nameOfReceiver": "string",
#             "Receiver_streetName": "string",
#             "Receiver_buildingNumber": "string",
#             "Receiver_city": "string",
#             "Receiver_postalCode": "string",
#             "Receiver_countryName": "string",
#             "Receiver_countryCode": "string",
#             "ReceivingSite_streetName": "string",
#             "ReceivingSite_buildingNumber": "string",
#             "ReceivingSite_city": "string",
#             "ReceivingSite_postalCode": "string",
#             "ReceivingSite_countryCode": "string",
#             "ReceivingSite_countryName": "string",
#         },
#         # na_values=NaValues,
#     )
#     data.where(
#         data != "CONFIDENTIAL",
#         pd.NA,
#         inplace=True,
#     )
#     if not case_sensitive_id:
#         data["Facility_INSPIRE_ID"] = data["Facility_INSPIRE_ID"].str.lower()
#     by_source: Final[list[str]] = [
#         "Facility_INSPIRE_ID",
#         "wasteClassificationCode",
#         "wasteTreatmentCode",
#     ]
#     levels: Final[list[str]] = ["reportingYear"] + by_source
#     if aggregate_by_source:
#         # aggregate by source across receivers
#         data = data.groupby(
#             levels,
#             observed=True,
#         )[["totalWasteQuantityTNE"]].sum(
#             min_count=1,
#         )
#         data.reset_index(inplace=True)
#     if balance_panel:
#         index = pd.MultiIndex.from_product(
#             [data[c].unique() for c in levels],
#             names=levels,
#         ).sort_values()
#         data = data.set_index(levels).reindex(index).reset_index()
#         # drop observations before first or after last reporting year
#         data = data.merge(
#             data.loc[data["totalWasteQuantityTNE"] > 0.0]
#             .groupby(
#                 by_source,
#                 observed=True,
#             )["reportingYear"]
#             .agg(["min", "max"])
#             .reset_index(),
#             on=by_source,
#             how="left",
#             validate="many_to_one",
#         )
#         do_keep = (data["reportingYear"] >= data["min"]) & (
#             data["reportingYear"] <= data["max"]
#         )
#         data.drop(data.index[~do_keep], inplace=True)
#     if layout == "wide":
#         data = data.pivot(
#             index=[
#                 "reportingYear",
#                 "Facility_INSPIRE_ID",
#             ],
#             columns=[
#                 "wasteClassificationCode",
#                 "wasteTreatmentCode",
#             ],
#             values="totalWasteQuantityTNE",
#         )
#         data.columns = ["_".join(c) for c in data.columns.to_flat_index()]
#     data.index.rename({"reportingYear": "year"}, inplace=True)
#     data.reset_index(inplace=True)
#     return data
