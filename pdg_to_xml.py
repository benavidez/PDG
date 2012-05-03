import csv
import random
import time
import sys
import urllib
import urllib2
import simplejson as json
#from invenio.search_engine import perform_request_search as search

"""
   Reads the pdg_codes file to produce a marcxml file. 
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
    """Move around the letter attached of a volume or page ref seeking unique

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
    letter = ''
    hits = []
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
            # print 'j,v,p: ' + j + ' ' + v + ' ' + p
            #  hits = list(search(p='find j ' + ','.join([j,v,p])))
            hits = get_inspire_id(j,v,p)
            if len(hits) > 0:
                return hits
    return hits
        

def get_inspire_id(journal,volume,pages):
    """
      Does a search in inspirehep.net
      Returns a list of one or more ids that match with journal, volume, and pages
    """
    search_str = 'find j ' + ','.join([journal,volume,pages])
    invenio_url = 'http://inspirehep.net/search?'
    data = {}        
    data['p'] = search_str
    data['of'] = 'id'
              
    url_value = urllib.urlencode(data) 
    full_url = invenio_url + url_value
       
    #print full_url    
    # full_url = 'http://inspirehep.net/search?ln=en&p=find+j+PRLTA&of=id'  
    return_id = urllib2.urlopen(full_url)
    hits =  return_id.read()
    
    #convert to list
    hits_list = json.loads(hits)
    #if hits_list == 0:
        #try again by moving the volume letter
        #  hits_list = move_around_letters(journal,volume,pages)
    #print str(hits_list[0]) + ' c=' + str(len(hits_list))

    return hits_list
    
    
def get_ref_hits_codes(line):
    """Return a search, its result set, and the set of PDG codes to attach

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

    # TODO read file from URL
    
    # read data from file, parsing w/ csv
    pdg_list = csv.reader(open('pdg_sm.csv', 'rb'), delimiter=',', quotechar='"', skipinitialspace=True)
    
    # TODO de-reference INSPIRE refs as we read them in
    
        
    # TODO turn refs in file into marcxml
    
    recs_found = 0
    recs_missing = 0
    recs_duplicates = 0
    recs_volume_mix = 0
    recs_total = 0
    
    DEBUGCOUNT = 0   
    marc_xml = open('marcxml.txt', 'wb')
    errmissingfile = open('missing.txt', 'wb')  
    print 'processing codeList...'
    
    xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + '<collection xmlns="http://www.loc.gov/MARC21/slim">\n'    
    missing_str = '#MISSING ON FIRST TRY\n'
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
            print 'hits_len:' + str(hits_len) + 'content: ' + str(hits[0])
            if hits_len < 1:
                print 'Missing:' + journal + ', ' + volume + ', ' + pages
                missing_str =  missing_str + journal + ',' + volume + ',' + pages + ',' + ','.join(codes) + '\n'
                recs_missing = recs_missing + 1
                    
        if hits_len > 1:                       
            print 'duplicate found:' + journal + ', ' + volume + ', ' + pages
            recs_duplicates = recs_duplicates + 1
        
        if hits_len == 1:
            print 'ok:' + str(hits[0])
            xml_str = xml_str + '   <record>\n      <controlfield tag=\'001\'>' + str(hits[0]) + '</controlfield>\n'
            for code in codes:
                xml_str = xml_str + '      <datafield tag=\'037\'><subfield code=\'z\'>' + code + '</subfield></datafield>\n'
            xml_str = xml_str + '   </record>\n'
            recs_found = recs_found + 1
        
        
    
         
            
        
            
    
    xml_str = xml_str + '</collection>\n</xml>'
    marc_xml.write(xml_str)
    errmissingfile.write(missing_str)
            
    
    #outfile = open('output.txt', 'wb')
    #errambfile = open('errors_amb.txt', 'wb')
  
    
    #outfile.close()
    #errambfile.close()
    errmissingfile.close()
    marc_xml.close()
    print 'done\n'
    print 'Found: ' + str(recs_found) + '\n'
    print 'Missing: ' + str(recs_missing) + '\n'
    print 'Vol permu: ' + str(recs_volume_mix) + '\n'    
    print 'Duplicates: ' + str(recs_duplicates) + '\n'    
    print 'Total: ' + str(recs_total) + '\n'    
    # TODO output marcxml for bibupload (update not replace)








if __name__ == '__main__':
    main()