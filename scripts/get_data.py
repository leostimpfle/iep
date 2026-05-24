import altair
from duckdb import DuckDBPyRelation

import iep

energy_input = iep.part.energyinput.load(sanitise=True)
emissions = iep.part.emissions.load(sanitise=True)

data = (
    energy_input.select(
        """reportingYear,
        Installation_Part_INSPIRE_ID,
        fuelInputCode,
        energyInputTJ,
        SUM(energyInputTJ) OVER (PARTITION BY reportingYear, Installation_Part_INSPIRE_ID) AS total,
        energyInputTJ / NULLIF(total, 0.0) AS share
        """
    )
    .filter("fuelInputCode = 'NaturalGas' AND share > 0.9")
    .join(
        emissions.filter("pollutantCode = 'NOX'").select(
            "reportingYear, Installation_Part_INSPIRE_ID, totalPollutantQuantityTNE"
        ),
        condition="reportingYear, Installation_Part_INSPIRE_ID",
        how="left",
    )
    .select("*, totalPollutantQuantityTNE / total AS ef")
    # .filter("reportingYear = 2019 AND ef > 0.0")
    # .order("ef")
)

energy_input.filter("fuelInputCode = 'NaturalGas'").select(
    """*,
    LAG(energyInputTJ) OVER (PARTITION BY Installation_Part_INSPIRE_ID, fuelInputCode ORDER BY reportingYear) AS lagged
    """
).select(
    """*,
    CASE WHEN energyInputTJ > 0.0 AND lagged > 0.0 THEN LOG(energyInputTJ / lagged) END AS log_change
    """
).filter("log_change NOT NULL AND reportingYear = 2019").aggregate(
    "ARGMAX(Installation_Part_INSPIRE_ID, log_change), MAX(log_change)"
)

f = "IT.CAED/100401001.PART"
f = "IT.CAED/570162008.PART"
emissions.filter(
    f"Installation_Part_INSPIRE_ID = '{f}' AND pollutantCode = 'NOX'"
).select("reportingYear, totalPollutantQuantityTNE").order("reportingYear")

altair.Chart(
    energy_input.aggregate("reportingYear, fuelInputCode, SUM(energyInputTJ) AS TJ")
).mark_line(point=True).encode(
    x="reportingYear:O", y=altair.Y("TJ:Q").axis(format="~s"), color="fuelInputCode:N"
).properties(width=800, height=800).save(
    r"/Users/leonardstimpfle/Downloads/energy.html"
)
groups = [
    "Installation_Part_Inspire_ID",
    "fuelInputCode",
    "otherSolidFuelCode",
    "otherGaseousFuelCode",
]
f = "https://registry.gdi-de.org/id/de.nw.inspire.pf.bube-eureg/avn-2017-366044-300-0079450-0100-0130"
f = "RS.SEPA.NRIZ/PART.000000046"
f = "IT.CAED/100401001.PART"
energy_input.filter(
    f"""Installation_Part_INSPIRE_ID = '{f}'
    AND fuelInputCode = 'NaturalGas'
    --AND reportingYear BETWEEN 2018 AND 2019
    """
).select(f"reportingYear, {', '.join(groups)}, energyInputTJ").order(
    "fuelInputCode, reportingYear"
)


def filter(data: DuckDBPyRelation, facility: str) -> DuckDBPyRelation:
    identifiers = iep.identifiers.load()
    return data.join(
        identifiers.filter(f"Facility_INSPIRE_ID = '{facility}'").select(
            "Installation_Part_INSPIRE_ID"
        ),
        condition="Installation_Part_INSPIRE_ID",
        how="inner",
    )


#
#
# facility = r"AT.CAED/9008390661741.FACILITY"
# parts = filter(iep.part.part.load(reload=False), facility=facility)
# details = filter(iep.part.details.load(), facility=facility)
# desulphurisation = filter(iep.part.desulphurisation.load(), facility=facility)
# derogations = filter(iep.part.derogations.load(), facility=facility)
# emissions = filter(iep.part.emissions.load(), facility=facility)
# energy_input = filter(iep.part.energyinput.load(), facility=facility)
#
#
# # %%
# fids = (
#     "PL.MŚ/000003639.FACILITY",
#     r"PL.MŚ/000003640.FACILITY",
#     r"PL.MŚ/000000114.FACILITY",
# )
# facilities = iep.facility.facility.load()
# print(facilities.filter(f"Facility_INSPIRE_ID = '{facility}'").select("nameOfFeature"))
pollutants = iep.facility.pollutant_release.load(
    deduplicate=True, sanitise=True, interpolate=True
)
# pollutants.filter(
#     f"Facility_INSPIRE_ID IN {fids} AND pollutantCode = 'CO2' AND medium = 'AIR'"
# ).select("reportingYear, totalPollutantQuantityKg").order("reportingYear")
