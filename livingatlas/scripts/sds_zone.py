import json

output = set()

with open('/data/sds-shp/sds_zones.json') as json_file:
  data = json.load(json_file)
  for p in data:
    if p['instances'] != None:
      if isinstance(p['instances'], list):
        for ci in p['instances']:
          if ci['@zone'] != None:
            zone = ci['@zone']
            output.add(zone)
      else:
        if  p['instances']['conservationInstance'] != None:
          ci = p['instances']['conservationInstance']
          if ci['@zone'] != None:
            zone = p['instances']['conservationInstance']['@zone']
            output.add(zone)
          #print zone

print output
