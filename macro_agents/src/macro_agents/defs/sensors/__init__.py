from macro_agents.defs.domains.macro import treasury_yields_sensor
from macro_agents.defs.domains.housing import realtor_gdrive_sensor

sensors = [treasury_yields_sensor, realtor_gdrive_sensor]

__all__ = [
    "realtor_gdrive_sensor",
    "treasury_yields_sensor",
    "sensors",
]
