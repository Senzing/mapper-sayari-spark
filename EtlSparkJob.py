import csv
import json
import string
import hashlib
import orjson
import sys
from pyspark.sql import SparkSession

punctuation_translations = str.maketrans('', '', string.punctuation)


def load_codes_file(codes_filename):
  code_conversion_data = {}
  unmapped_code_count = 0
  with open(codes_filename, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
      row['RAW_TYPE'] = row['RAW_TYPE'].upper()
      row['RAW_CODE'] = row['RAW_CODE'].upper()
      if row['RAW_TYPE'] not in code_conversion_data:
        code_conversion_data[row['RAW_TYPE']] = {}
      row['COUNT'] = 0
      row['EXAMPLES'] = {}
      code_conversion_data[row['RAW_TYPE']][row['RAW_CODE']] = row
      if row['REVIEWED'].upper() != 'Y':
        unmapped_code_count += 1
  return code_conversion_data, unmapped_code_count,


def clean_str(str_val):
  # return str_val
  return ''.join(
    str(str_val.translate(punctuation_translations)).split()).upper()


def get_with_default(self, _data, _attr, _default=''):
  value = _data.get(_attr)
  if value:
    return value
  else:
    return _default


def get_extra_attribute(self, _data, _attr, _default=''):
  if _data.get('extra'):
    _attr = _attr.upper()
    for extra_attr in _data['extra']:
      if extra_attr[0].upper() == _attr:
        return extra_attr[1].upper()
  return _default.upper()


# will return string with json
def reduce_by_entity_id_to_json(entityId, aggregations):
  record_id = entityId
  entity_type = aggregations[0]['type'].upper()
  code_conversion_data = json.load(aggregations[0]['code_conversion_data'])
  record_type = code_conversion_data['ENTITY_TYPE'][entity_type][
    'SENZING_DEFAULT']
  # accumulate attributes for de-duplication
  temp_data = {}
  temp_data['attribute_list'] = []
  temp_data['contact_methods'] = []
  temp_data['identifier_list'] = []
  temp_data['distinct_name_list'] = {}
  temp_data['distinct_addr_list'] = {}
  temp_data['payload_data_list'] = []

  if aggregations[0]['label']:
    clean_name = clean_str(aggregations[0]['label'])
    temp_data['distinct_name_list'][clean_name] = ['PRIMARY',
                                                   aggregations[0]['label']]

  if aggregations[0]['label_en']:
    clean_name = clean_str(aggregations[0]['label_en'])
    if clean_name not in temp_data['distinct_name_list']:
      temp_data['distinct_name_list'][clean_name] = ['PRIMARY_EN',
                                                     aggregations[0][
                                                       'label_en']]
    else:
      aggregations[0].update_stat('!TRUNCATIONS', 'duplicateNames', record_id)

  if aggregations[0]['sanctioned']:
    temp_data['payload_data_list'].append(
      {"sanctioned": 'Yes' if aggregations[0]['sanctioned'] else 'No'})

  if aggregations[0]['pep']:
    temp_data['payload_data_list'].append(
      {"pep": 'Yes' if aggregations[0]['pep'] else 'No'})

  if aggregations[0]['closed']:
    temp_data['payload_data_list'].append({"closed": aggregations[0]['closed']})

  for source in aggregations[0]['sources']:
    temp_data['payload_data_list'].append({"source": source})

  for name_data in aggregations[0]['name']:
    name = name_data.get('value')
    if name:
      raw_name_type = get_with_default(name_data, 'context', 'other_name')[
                      0:50].upper()
      name_type = code_conversion_data['NAME_TYPE'][raw_name_type][
        'SENZING_DEFAULT']
      clean_name = clean_str(name)
      if clean_name not in temp_data['distinct_name_list']:
        temp_data['distinct_name_list'][clean_name] = [name_type, name]

  for address_data in aggregations[0]['address']:
    address = address_data.get('value')
    if address:
      raw_addr_type = get_extra_attribute(address_data, 'Address Type',
                                          'other_address')[0:50].upper()
      addr_type = code_conversion_data['ADDRESS_TYPE'][raw_addr_type][
        'SENZING_DEFAULT']
      if not addr_type and entity_type != 'PERSON':  # force unique for non-persons if not specified
        addr_type = 'BUSINESS'
      if addr_type == 'BUSINESS' and entity_type == 'PERSON':  # make sure persons do not mistakenly get unique addresses
        addr_type = raw_addr_type
      if addr_type == 'PLACE_OF_BIRTH':
        temp_data['attribute_list'].append({'PLACE_OF_BIRTH': address})
      else:
        clean_addr = clean_str(address)
        if clean_addr not in temp_data['distinct_addr_list']:
          temp_data['distinct_addr_list'][clean_addr] = [addr_type, address]

    # these remarks may include PII!
    addr_remarks = get_extra_attribute(address_data, 'remarks')
    if addr_remarks:
      temp_data['payload_data_list'].append({'address_remarks': addr_remarks})

  for dob_data in aggregations[0]['date_of_birth']:
    dob = dob_data.get('value')
    if dob:
      temp_data['attribute_list'].append({'DATE_OF_BIRTH': dob})

  for gender_data in aggregations[0]['gender']:
    gender = gender_data.get('value')
    if gender:
      temp_data['attribute_list'].append({'GENDER': gender})

  for contact_data in aggregations[0]['contact']:
    contact_value = contact_data.get('value', None)
    if contact_value:
      raw_contact_type = get_with_default(contact_data, 'type',
                                          'unknown_contact_type')[0:50].upper()
      senzing_attr = code_conversion_data['CONTACT_TYPE'][raw_contact_type][
        'SENZING_ATTR']
      senzing_attr_type = \
        code_conversion_data['CONTACT_TYPE'][raw_contact_type][
          'SENZING_DEFAULT']
      mapped_data = {senzing_attr: contact_value}
      if senzing_attr_type:
        if senzing_attr == 'PHONE_NUMBER':
          mapped_data = {'PHONE_TYPE': senzing_attr_type}
      if senzing_attr.startswith('UNKNOWN_CONTACT_TYPE'):
        temp_data['payload_data_list'].append(mapped_data)
      else:
        temp_data['contact_methods'].append(mapped_data)

  idtype_counts = {}
  for identifier_data in aggregations[0]['identifier']:
    id_value = identifier_data.get('value', None)
    if id_value:
      raw_id_type = get_with_default(identifier_data, 'type', 'unknown_id')[
                    0:50].upper()
      if raw_id_type not in idtype_counts:
        idtype_counts[raw_id_type] = 1
      else:
        idtype_counts[raw_id_type] += 1

      senzing_attr = code_conversion_data['IDENTIFIER_TYPE'][raw_id_type][
        'SENZING_ATTR']
      senzing_attr_type = code_conversion_data['IDENTIFIER_TYPE'][raw_id_type][
        'SENZING_DEFAULT']

      if senzing_attr in (
          'OTHER_ID', 'NATIONAL_ID', 'TAX_ID', 'DRIVERS_LICENSE', 'PASSPORT'):
        mapped_data = {senzing_attr + '_NUMBER': id_value}
        if senzing_attr_type:
          if senzing_attr != 'DRIVERS_LICENSE':
            mapped_data[senzing_attr + '_COUNTRY'] = senzing_attr_type
          else:
            mapped_data[senzing_attr + '_STATE'] = senzing_attr_type
        if senzing_attr in ('OTHER_ID',
                            'NATIONAL_ID'):  # --, 'NATIONAL_ID') not an attribute at this time
          mapped_data[senzing_attr + '_TYPE'] = raw_id_type
        temp_data['identifier_list'].append(mapped_data)
      elif senzing_attr:
        temp_data['identifier_list'].append({senzing_attr: id_value})
      else:
        # this can happen when an idtype is registered in sayari_codes.csv and is left unmapped
        # it is done on purpose as some identifiers are dates or categories
        # when you find an identifier code is not really an identifier, you should make it payload by
        # leaving its mapping blank
        temp_data['payload_data_list'].append({raw_id_type: id_value})

  for country_data in aggregations[0]['country']:
    country_value = country_data.get('value')
    if country_value:
      raw_country_context = get_with_default(country_data, 'context',
                                             'related_country')[0:50].upper()
      senzing_attr = \
        code_conversion_data['COUNTRY_CONTEXT'][raw_country_context][
          'SENZING_ATTR']
      if {senzing_attr: country_value} not in temp_data[
        'attribute_list']:  # can get dupes since most map to country_of_assocation
        temp_data['attribute_list'].append({senzing_attr: country_value})

  for status_data in aggregations[0]['status']:
    value = status_data.get('value')
    if not value:
      value = status_data.get('text')
    date = status_data.get('date')
    context = status_data.get('context')
    if value and value.upper() == 'REGISTERED' and date:
      temp_data['attribute_list'].append({"REGISTRATION_DATE": date})
    elif value:
      status_attr = 'Status' if not context else context + ' status'
      if date:
        value = f"{value} ({date})"
      temp_data['payload_data_list'].append({status_attr: value})

  for company_type_data in aggregations[0]['company_type']:
    value = company_type_data.get('value')
    if value:
      temp_data['payload_data_list'].append({'company_type': value})

  for business_data in aggregations[0]['business_purpose']:
    value = business_data.get('value')
    code = business_data.get('code')
    if code and value:
      standard = business_data.get('standard')
      if standard:
        temp_data['payload_data_list'].append({standard: f"{code}={value}"})
      else:
        temp_data['payload_data_list'].append({code: value})

  if payload_level == 'A':

    if aggregations[0]['num_documents']:
      aggregations[0]['payload_data_list'].append(
        {"num_documents": aggregations[0]['num_documents']})

    if aggregations[0]['degree']:
      temp_data['payload_data_list'].append(
        {"degree": aggregations[0]['degree']})

    if aggregations[0]['risk_factors']:
      for risk_factor_key in aggregations[0]['risk_factors'].keys():
        if aggregations[0]['risk_factors'][risk_factor_key].get('value'):
          temp_data['payload_data_list'].append({
            f"risk_factor-{risk_factor_key}":
              aggregations[0][
                'risk_factors'][
                risk_factor_key].get(
                'value')})

    if aggregations[0]['edge_counts']:
      for edge_data in aggregations[0]['edge_counts']:
        if edge_data[0]:
          cnt_in = edge_data[1].get('in', 0)
          cnt_out = edge_data[1].get('out', 0)
          temp_data['payload_data_list'].append(
            {f"edge-{edge_data[0]}": f"{cnt_in} in, {cnt_out} out"})

    for additional_data in aggregations[0]['additional_information']:
      adtype = additional_data.get('type')
      advalue = additional_data.get('value')
      if adtype and advalue:
        temp_data['payload_data_list'].append({adtype: advalue})

    for finance_data in aggregations[0]['finances']:
      value = finance_data.get('value')
      context = finance_data.get('context')
      currency = finance_data.get('currency')
      if context and value:
        if currency:
          value = f"{value} {currency}"
        temp_data['payload_data_list'].append({f"finances-{context}": value})

    for share_data in aggregations[0]['shares']:
      num_shares = share_data.get('num_shares')
      shtype = share_data.get('type')
      percentage = share_data.get('percentage')
      if shtype and num_shares:
        if percentage:
          num_shares = f"{num_shares} ({percentage}%)"
        temp_data['payload_data_list'].append({f"{shtype} shares": num_shares})
  # putting it all together

  json_data = {}
  json_data['DATA_SOURCE'] = 'SAYARI'
  json_data['RECORD_TYPE'] = record_type

  # set organization name attribute and first address label
  if record_type == 'PERSON':
    name_attr = 'NAME_FULL'
  else:
    name_attr = 'NAME_ORG'
  if record_type == 'ORGANIZATION':
    default_addr_label = 'BUSINESS'
  else:
    default_addr_label = 'PRIMARY'

  if temp_data['distinct_name_list']:
    json_data['NAME_LIST'] = []
    for clean_name in temp_data['distinct_name_list']:
      this_type, this_name = temp_data['distinct_name_list'][clean_name]
      if not this_type:
        this_type = 'ADDITIONAL'
      if len(this_name.split()) > 15:  # ignores names with more than 15 tokens
        # update_stat('TRUNCATIONS', 'longNameCnt', record_id + ' [' + this_name + ']')
        this_name = ' '.join(this_name.split()[:14]) + ' <truncated>'
      else:
        json_data['NAME_LIST'].append(
          {'NAME_TYPE': this_type, name_attr: this_name})
    if len(json_data['NAME_LIST']) > 25:
      # update_stat('TRUNCATIONS', 'tooManyNames', record_id + ' [' + str(len(temp_data['distinct_name_list'])) + ']')
      json_data['NAME_LIST'] = json_data['NAME_LIST'][0:25]
    json_data['NAME_LIST'] = json_data['NAME_LIST']

  if temp_data['distinct_addr_list']:
    json_data['ADDRESS_LIST'] = []
    for clean_addr in temp_data['distinct_addr_list']:
      this_type, this_addr = temp_data['distinct_addr_list'][clean_addr]
      if not this_type:
        this_type = default_addr_label
        if record_type != 'ORGANIZATION':  # all organization addresses should remain BUSINESS
          default_addr_label = 'ADDITIONAL'

      json_data['ADDRESS_LIST'].append(
        {'ADDR_TYPE': this_type, 'ADDR_FULL': this_addr})
    json_data['ADDRESS_LIST'] = json_data['ADDRESS_LIST']

  if temp_data['attribute_list']:
    json_data['ATTRIBUTE_LIST'] = temp_data['attribute_list']
  if temp_data['contact_methods']:
    json_data['CONTACT_METHODS'] = temp_data['contact_methods']
  if temp_data['identifier_list']:
    json_data['IDENTIFIER_LIST'] = temp_data['identifier_list']

  if temp_data['payload_data_list'] and payload_level != 'N':
    for payload_attribute in temp_data['payload_data_list']:
      attr_name = list(payload_attribute.keys())[0]
      if attr_name not in json_data:
        json_data[attr_name] = payload_attribute[attr_name]
      elif str(payload_attribute[attr_name]) not in str(json_data[attr_name]):
        json_data[attr_name] = str(json_data[attr_name]) + ' | ' + str(
          payload_attribute[attr_name])

  # compute record hash with this data
  base_json_string = orjson.dumps(json_data, option=orjson.OPT_SORT_KEYS)
  record_hash = hashlib.md5(base_json_string).hexdigest()
  json_data['RELATIONSHIPS'] = [{
    'REL_ANCHOR_DOMAIN': 'SAYARI',
    'REL_ANCHOR_KEY': record_id
  }]
  relationship_count = 0
  for relation_row in aggregations:
    relationship_count += 1
    rel_pointer_data = {'REL_POINTER_DOMAIN': 'SAYARI',
                        'REL_POINTER_KEY': relation_row['dst'],
                        'REL_POINTER_ROLE': relation_row['type']
                        }
    if relation_row[3]:
      rel_pointer_data['REL_POINTER_FROM_DATE'] = relation_row['from_date']
    if relation_row[4]:
      rel_pointer_data['REL_POINTER_THRU_DATE'] = relation_row['to_date']
    json_data['RELATIONSHIPS'].append(rel_pointer_data)
  return json.dump(json_data)


if __name__ == "__main__":
  spark = SparkSession.builder.appName(
    "Etl parquet aggregation to json").getOrCreate()

  payload_level = ''
  # with s3a:// prefix
  df_entities = spark.read.parquet(sys.argv[1])
  df_relations = spark.read.parquet(sys.argv[2])
  analysis_mode = False
  other_sample_size = 100
  id_sample_size = 1000000 if analysis_mode else other_sample_size
  codes_filename = sys.argv[3]
  code_conversion_data, unmapped_code_count = load_codes_file(codes_filename)
  joined_data_frame = df_entities.join(df_relations,
                                       df_entities.entity_id == df_relations.src,
                                       "left")
  joined_data_frame.withColumn("code_conversion_data",
                               json.dump(code_conversion_data))
  joined_data_frame = joined_data_frame.rdd.map(
    lambda x: (x.entity_id, x)).groupByKey().mapValues(list). \
    reduceByKey(lambda x, y: reduce_by_entity_id_to_json(x, y))
  joined_data_frame.saveAsTextFile(sys.argv[4])
