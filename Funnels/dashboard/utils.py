def calculate_liquidity_score(lead):
    """Calculate a 0-100 Liquidity Score based on Chilean market data."""
    score = 0
    title = str(lead.get("title", "")).lower()
    
    # 1. Brand & Model (Max 40)
    tier1 = ["swift", "morning", "sail", "gol", "yaris", "soluto", "208", "accent", "rio", "spark"]
    tier2 = ["l200", "hilux", "t60", "groove", "rav4", "cx-5", "tiggo 2", "f-150", "kicks", "tracker"]
    tier3 = ["tucson", "santa fe", "qashqai", "versa", "baleno", "vitara", "mazda 3", "mazda 6", "ecosport"]
    tier4 = ["bmw", "mercedes", "audi", "jeep", "dodge", "porsche"]
    
    if any(t in title for t in tier1): score += 40
    elif any(t in title for t in tier2): score += 30
    elif any(t in title for t in tier3): score += 20
    elif any(t in title for t in tier4): score += 10
    
    # 2. Year (Max 20)
    year = lead.get("year")
    if year:
        try:
            year = int(year)
        except (ValueError, TypeError):
            year = None
    if year:
        if year >= 2020: score += 20
        elif year >= 2017: score += 15
        elif year >= 2013: score += 10
        elif year >= 2008: score += 5

    # 3. Mileage (Max 20) - km string format (e.g. "64K km")
    km_str = str(lead.get("mileage", "999")).replace("K", "000").replace(" km", "").replace(",", "")
    try:
        km = int(km_str)
        if km <= 50000: score += 20
        elif km <= 90000: score += 15
        elif km <= 130000: score += 10
        elif km <= 180000: score += 5
    except ValueError:
        pass
        
    # 4. Price (Max 20)
    price_str = str(lead.get("price", "CLP99999999")).replace("CLP", "").replace(",", "").replace(" ", "")
    try:
        price = int(price_str)
        if 4000000 <= price <= 10000000: score += 20
        elif 10000001 <= price <= 18000000: score += 15
        elif price < 4000000: score += 10
        elif 18000001 <= price <= 25000000: score += 10
        elif price > 25000000: score += 5
    except ValueError:
        pass
        
    return score


def get_region_data(location_str):
    """Determine if a location is in V Region and estimate km distance from Viña del Mar."""
    loc = str(location_str).lower().strip()
    
    # Map of V Region communes to approx km distance from Viña del Mar
    v_region = {
        "viña del mar": 0, "vina del mar": 0,
        "concón": 15, "concon": 15,
        "valparaíso": 10, "valparaiso": 10,
        "quilpué": 18, "quilpue": 18,
        "villa alemana": 22,
        "quintero": 35,
        "limache": 40,
        "olmué": 45, "olmue": 45,
        "casablanca": 45,
        "quillota": 50,
        "la cruz": 55,
        "puchuncaví": 60, "puchuncavi": 60,
        "calera": 65,
        "nogales": 70,
        "hijuelas": 75,
        "algarrobo": 80,
        "el quisco": 85,
        "el tabo": 90,
        "san antonio": 100,
        "cartagena": 100,
        "santo domingo": 110,
    }
    
    for commune, distance in v_region.items():
        if commune in loc:
            return {"is_v_region": True, "distance_to_vina": distance}
            
    return {"is_v_region": False, "distance_to_vina": 9999}
