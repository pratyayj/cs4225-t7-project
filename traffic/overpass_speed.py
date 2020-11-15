import overpy
import sys
import simplejson as json
import math
from decimal import Decimal


# Queries the Overpass API for speed limits of a road, given lat lon coordinates
def overpy_speed_limit(lat, lon):
    api = overpy.Overpass()

    # Get all speed limits within the radius of 300m. If no roads found, increase radius by 100 and query again, until 1000m.
    for radius in range(300, 1000, 100):
        result = api.query("""
                way(around:""" + str(radius) + """,""" + lat  + """,""" + lon  + """) ["maxspeed"];
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
        
        if len(results_list) > 0:
            break
    # print(json.dumps(results_list))

    # If no roads found, default to max speed limit of austin 60mph.
    if len(results_list) == 0:
        return 60
    
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
        for node in road["nodes"]:
            dist = haversine_dist(float(lat), float(lon), float(node[0]), float(node[1]))
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_limit = road["speed_limit"]

    return int(nearest_limit.split(' ')[0])


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


def get_speed_limit_mapping(infile, outfile):
    infile.readline() # skip the first line of column names
    outfile.write('atd_device_id,speed_limit')
    outfile.write('\n')

    while True:
        line = infile.readline()
        if not line:
            break
        
        tokens = line.rstrip('\n').split(',')
        detector_id = tokens[0]
        lat, lon = tokens[-2:]

        speed_limit = overpy_speed_limit(lat, lon)
        print(detector_id + ',' + str(speed_limit))
        outfile.write(detector_id + ',' + str(speed_limit))
        outfile.write('\n')


def main():
    # lat = sys.argv[1]
    # lon = sys.argv[2]
    # radius = sys.argv[3]
    # result = overpy_speed_limit(lat, lon, radius)
    # print(result)

    with open(sys.argv[1], 'r') as infile:
        with open(sys.argv[2], 'w') as outfile:
            get_speed_limit_mapping(infile, outfile)


if __name__ == "__main__":
    main()
