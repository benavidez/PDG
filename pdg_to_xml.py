import csv
import random
import time
import urllib
import urllib2
import simplejson as json
#from invenio.search_engine import perform_request_search as search

"""
   Read the pdg_codes file to produce a marcxml file. 
   
   Each record is verified that it exists on INSPIRE (find j PRLTA,54,2489), 
   and the record id is used in the marcxml file. 
   
   From: PRLTA,54,2489,S035:Desig=20,S035:Desig=3,S035:Desig=4
   To:
    <record>
      <controlfield tag='001'>213362</controlfield>
      <datafield tag='037'><subfield code='z'>s035:desig=20</subfield></datafield>
      <datafield tag='037'><subfield code='z'>s035:desig=3</subfield></datafield>
      <datafield tag='037'><subfield code='z'>s035:desig=4</subfield></datafield>
    </record>
    Record syntax istill to be verified!!!
"""


def move_around_letters(journal,volume,pages):
    """
    Move around the letter attached of a volume or page ref seeking unique

    Sometimes volume letters for a coden reference get put in the wrong place:
    they're on the page when they should be on the volume, or they're at the
    front of the volume when they should be at the back (or vice versa).  
    Here we try moving the letters around until we've gotten exactly 0 hits for
    various permutations or we find a unique result.

    If we find no unique result, throw the volume letter away altogether.
    """

    def permutations(journal,volume,pages,letter):
        yield journal,letter+volume,pages
        yield journal,volume+letter,pages
        yield journal,volume,letter+pages
        yield journal,volume,pages+letter

    letters = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
    letter  = ''
    hits    = []
    
    if volume[0] in letters:
        letter = volume[0]
        volume = volume[1:]
    elif volume[-1] in letters:
        letter = volume[-1]
        volume = volume[:-1]
    elif pages[0] in letters:
        letter = pages[0]
        pages = pages[1:]
    elif pages[-1] in letters:
        letter = pages[-1]
        pages = pages[:-1]
        
    if letter != '':
        for j,v,p in permutations(journal,volume,pages,letter):           
            hits = get_inspire_id(j,v,p)
            if len(hits) > 0:
                return hits
            
    return hits
        

def get_inspire_id(journal,volume,pages):
    """
      Search in inspirehep.net for record id
      
      Return a list of one or more ids that match with journal, volume, and pages
    """
    
    search_str   = 'find j ' + ','.join([journal,volume,pages])
    invenio_url  = 'http://inspirehep.net/search?'
    data = {'p': search_str, 'of': 'id'}
    hits_handle = urllib2.urlopen(invenio_url + urllib.urlencode(data))
    hits = json.loads(hits_handle.read())
    
    return hits
    
    
def get_ref_hits_codes(line):
    """
    Return a search, its result set, and the set of PDG codes to attach

    Returns a tuple consisting of
    * hits - the set of records responsive to the reference search
             may be a LIST or NONE
    * codes - the collection of PDG code designations to be assigned to the 
              unique record specified by hits.  maybe a LIST of STRINGS or NONE
    * journal, volume, and pages
    """

    hits         = None
    journal      = line[0].strip().upper()
    if journal.startswith('#'):             # skip comments
        return None, None, None, None, None
    
    volume,pages = [x.strip().upper() for x in line[1:3]]
    codes        = [x.strip().lower() for x in line[3:]]
    
    if volume or pages:
        hits = get_inspire_id(journal,volume,pages)       
                    
    return codes, hits, journal, volume, pages
  
  
  
  
def main():

    # read data from file, parsing w/ csv
    pdg_list        = csv.reader(open('pdg_sm.csv', 'rb'), delimiter=',', quotechar='"', skipinitialspace=True)    
    recs_found      = 0
    recs_missing    = 0
    recs_duplicates = 0
    recs_volume_mix = 0
    recs_total      = 0
    DEBUGCOUNT      = 0   
    marc_xml        = open('marcxml.txt', 'wb')
    errmissingfile  = open('needed_a_massage.txt', 'wb')      
    xml_str         = '<?xml version="1.0" encoding="UTF-8"?>\n' + '<collection xmlns="http://www.loc.gov/MARC21/slim">\n'    
    missing_str     = '#MISSING ON FIRST TRY, SO HAD TO MOVE VOL LETTER\n'
    
    print 'processing codeList...'    
    for pdg_line in pdg_list:
        hits = None
        DEBUGCOUNT += 1
        if DEBUGCOUNT % 100 == 0: 
            time.sleep(random.randint(1,9))
            print "100 records processed"
    
        codes, hits, journal, volume, pages = get_ref_hits_codes(pdg_line)
        
        #When journal is None, line was a comment. Otherwise, count as record
        if journal != None: recs_total = recs_total + 1
        hits_len = len(hits)
        if hits_len < 1:                                  
            #Missing, so try again
            missing_str =  missing_str + journal + ',' + volume + ',' + pages + ',' + ','.join(codes) + '\n'      
            recs_volume_mix = recs_volume_mix + 1
            hits = move_around_letters(journal,volume,pages)
            hits_len = len(hits) 
            print 'hits_len:' + str(hits_len) + '\n content: ' + str(hits[0])
            if hits_len < 1:
                print 'Missing:' + journal + ', ' + volume + ', ' + pages
                missing_str =  missing_str + journal + ',' + volume + ',' + pages + ',' + ','.join(codes) + '\n'
                recs_missing = recs_missing + 1
                    
        if hits_len > 1:                       
            print 'duplicate found:' + journal + ', ' + volume + ', ' + pages
            recs_duplicates = recs_duplicates + 1
        
        if hits_len == 1:
            print 'ok:' + str(hits[0])
            xml_str = xml_str + '   <record>\n      <controlfield tag=\'001\'>' + str(hits[0]) + '</controlfield>'

            for code in codes:
                xml_str = xml_str + '\n      <datafield tag=\'084\'>' + '\n         <subfield code=\'a\'>' + code + \
                                    '</subfield>' + '\n         <subfield code=\'2\'>PDG</subfield>' + \
                                    '\n         <subfield code=\'9\'>PDG</subfield>\n      </datafield>'
            xml_str = xml_str + '\n   </record>\n'
            recs_found = recs_found + 1
    
    
    xml_str = xml_str + '</collection>\n</xml>'
    marc_xml.write(xml_str)
    marc_xml.close()
    
    errmissingfile.write(missing_str)    
    errmissingfile.close()
    
    print 'done processing. \n'
    print 'Found: ' + str(recs_found) + '\n'
    print 'Missing: ' + str(recs_missing) + '\n'
    print 'Vol Manipulation: ' + str(recs_volume_mix) + '\n'    
    print 'Duplicates: ' + str(recs_duplicates) + '\n'    
    print 'Total: ' + str(recs_total) + '\n'    
        


if __name__ == '__main__':
    main()
