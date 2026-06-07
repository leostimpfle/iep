import altair

import iep

# %% energy inputs
raw = iep.part.energy_input._load_raw()
sanitised = iep.part.energy_input.load(sanitise=True)
combined = raw.select(
    "'raw' AS source, reportingYear, Installation_Part_INSPIRE_ID, fuelInputCode, energyInputTJ"
).union(
    sanitised.select(
        "'sanitised' AS source, reportingYear, Installation_Part_INSPIRE_ID, fuelInputCode, energyInputTJ"
    )
)
altair.Chart(
    combined.filter("fuelInputCode = 'NaturalGas'").aggregate(
        "reportingYear, source, SUM(energyInputTJ) AS energyInputTJ"
    )
).mark_line().encode(
    x="reportingYear:O",
    y=altair.Y("energyInputTJ:Q").axis(format="~s"),
    color="source:N",
)


# %% pollutant release
raw = iep.facility.pollutant_release._load_raw()
sanitised = iep.facility.pollutant_release.load(deduplicate=True, sanitise=True)
combined = raw.select(
    "'raw' AS source, reportingYear, Facility_INSPIRE_ID, pollutantCode, medium, totalPollutantQuantityKg"
).union(
    sanitised.select(
        "'sanitised' AS source, reportingYear, Facility_INSPIRE_ID, pollutantCode, medium, totalPollutantQuantityKg"
    )
)
altair.Chart(
    combined.filter("pollutantCode = 'CO2' AND medium = 'AIR'").aggregate(
        "reportingYear, source, SUM(totalPollutantQuantityKg) AS totalPollutantQuantityKg"
    )
).mark_line().encode(
    x="reportingYear:O",
    y=altair.Y("totalPollutantQuantityKg:Q").axis(format="~s"),
    color="source:N",
)
