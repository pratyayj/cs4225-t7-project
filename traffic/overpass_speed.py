import overpy
import sys
import simplejson as json
import math
from decimal import Decimal


#pip install overpy
#python overpass_speed.py 37.7833 -122.4167 500

def overpy_speed_limit(coordinates, radius):
    lat, lon = coordinates
    api = overpy.Overpass()

    # Get all speed limits within the radius
    result = api.query("""
            way(around:""" + radius + """,""" + lat  + """,""" + lon  + """) ["maxspeed"];
                (._;>;);
                    out body;
                        """)
    results_list = []
    for way in result.ways:
        road = {}
        road["name"] = way.tags.get("name", "")
        road["speed_limit"] = way.tags.get("maxspeed", "")
        # Ignore roads that dont have recorded speed limits
        if road["speed_limit"] == "":
            continue
        nodes = []
        for node in way.nodes:
            nodes.append((node.lat, node.lon))
        road["nodes"] = nodes
        results_list.append(road)
    
    # Pick the one most likely to be relevant. We filter by the conditions below in this order:
    # 1. Has a name
    # 2. Has the nearest node
    candidates = []
    for road in results_list:
        if not road["name"] == "":
            candidates.append(road)

    if len(candidates) == 0:
        candidates = results_list

    # print(json.dumps(candidates))
    nearest_limit = -1
    nearest_dist = sys.maxsize
    for road in candidates:
        print(json.dumps(road))
        for node in road["nodes"]:
            dist = haversine_dist(float(lat), float(lon), float(node[0]), float(node[1]))
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_limit = road["speed_limit"]

    return nearest_limit


# === Helper functions ===
def deg2rad(deg):
  return deg * (math.pi/180)

def haversine_dist(lat1,lon1,lat2,lon2):
  R = 6371 #Radius of the earth in km
  dLat = deg2rad(lat2-lat1)
  dLon = deg2rad(lon2-lon1) 
  a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
  c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
  d = R * c #Distance in km
  return d

# === End helper functions ===


def get_speed_limit_mapping(infile, outfile, radius):
    while True:
        line = infile.readline()
        if not line:
            break
        
        tokens = line.rstrip('\n').split(',')
        detector_id, lat, lon = tokens
        result = overpy_speed_limit((lat, lon), radius)
        speed_limit = result.split(' ')[0]
        outfile.write(detector_id + ',' + speed_limit)
        outfile.write('\n')


def main():
    # lat = sys.argv[1]
    # long = sys.argv[2]
    radius = sys.argv[3]
    # result = overpy_speed_limit((lat, lon), radius)
    # print(result)

    with open(sys.argv[1], 'r') as infile:
        with open(sys.argv[2], 'w') as outfile:
            get_speed_limit_mapping(infile, outfile, radius)


if __name__ == "__main__":
    main()
