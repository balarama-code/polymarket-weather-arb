"""
Forecast Arb Engine — Configuration
"""

# Capital
INITIAL_CAPITAL = 100.0       # Dry-run starting capital (USD)
MAX_POSITION_SIZE = 2.0       # Max per single trade ($2)
KELLY_FRACTION = 0.25         # Quarter-Kelly for safety
MIN_EDGE = 0.05               # Minimum 5% edge to fire
MAX_EXPOSURE = 0.30           # Max 30% of capital exposed at once
MAX_POSITIONS = 8             # Max simultaneous positions

# Timing
WEATHER_CHECK_INTERVAL = 300  # Seconds between forecast cycles
CYCLE_SPEED_DRY_RUN = 2       # Seconds between cycles in dry-run

# APIs
OPEN_METEO_API = "https://api.open-meteo.com/v1"
OPEN_METEO_HIST = "https://archive-api.open-meteo.com/v1/archive"
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"

# Weather models (simulated via Open-Meteo ensemble members)
WEATHER_MODELS = {
    "ECMWF":  {"source": "ecmwf_ifs025",  "latency_ms": 252, "accuracy": 0.863},
    "GFS":    {"source": "gfs_seamless",   "latency_ms": 61,  "accuracy": 0.988},
    "HRRR":   {"source": "gfs_seamless",   "latency_ms": 71,  "accuracy": 0.848},
    "NAM":    {"source": "gfs_seamless",   "latency_ms": 175, "accuracy": 0.841},
    "UKMO":   {"source": "ukmo_seamless",  "latency_ms": 222, "accuracy": 0.867},
    "CMC":    {"source": "gem_seamless",   "latency_ms": 155, "accuracy": 0.973},
}

# Cities to track
CITIES = {
    "Chicago":      {"lat": 41.88, "lon": -87.63},
    "New York":     {"lat": 40.71, "lon": -74.01},
    "Miami":        {"lat": 25.76, "lon": -80.19},
    "Los Angeles":  {"lat": 34.05, "lon": -118.24},
    "Houston":      {"lat": 29.76, "lon": -95.37},
    "Denver":       {"lat": 39.74, "lon": -104.99},
    "Seattle":      {"lat": 47.61, "lon": -122.33},
    "Phoenix":      {"lat": 33.45, "lon": -112.07},
}

# Weather contract types
CONTRACT_TYPES = [
    {"name": "Frost.Wheat",    "city": "Chicago",     "metric": "temperature_2m_min", "threshold": 0,   "direction": "below", "category": "frost"},
    {"name": "Freeze.OJ",      "city": "Miami",       "metric": "temperature_2m_min", "threshold": 2,   "direction": "below", "category": "frost"},
    {"name": "Heat.Corn",      "city": "Houston",     "metric": "temperature_2m_max", "threshold": 38,  "direction": "above", "category": "heat"},
    {"name": "Wind.Solar",     "city": "Los Angeles", "metric": "wind_speed_10m_max", "threshold": 50,  "direction": "above", "category": "wind"},
    {"name": "Snow.NYC",       "city": "New York",    "metric": "snowfall",           "threshold": 5,   "direction": "above", "category": "snow"},
    {"name": "Drought.Soy",    "city": "Chicago",     "metric": "precipitation",      "threshold": 1,   "direction": "below", "category": "drought"},
    {"name": "Fog.Air",        "city": "Seattle",     "metric": "visibility",         "threshold": 1000,"direction": "below", "category": "fog"},
    {"name": "Hail.Crop",      "city": "Denver",      "metric": "cape",               "threshold": 1500,"direction": "above", "category": "storm"},
    {"name": "Monsoon.Rice",   "city": "Houston",     "metric": "precipitation",      "threshold": 30,  "direction": "above", "category": "rain"},
    {"name": "NatGas.Wtr",     "city": "Chicago",     "metric": "temperature_2m_min", "threshold": -5,  "direction": "below", "category": "frost"},
    {"name": "Cyclone.Oil",    "city": "Miami",       "metric": "wind_speed_10m_max", "threshold": 80,  "direction": "above", "category": "cyclone"},
    {"name": "ElNino.Cocoa",   "city": "Miami",       "metric": "temperature_2m_max", "threshold": 33,  "direction": "above", "category": "heat"},
]
