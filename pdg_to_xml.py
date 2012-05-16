import csv
import random
import time
import urllib
import urllib2
import simplejson as json

"""
Read the pdg_codes file to produce a marcxml file.
Each record is verified that it exists on INSPIRE (find j PRLTA,54,2489),
and the record id is used in the marcxml file.
From: PHRVA,D32,2468,S035RHE,S035RHM,S035RHO
To:
 <record>
    <controlfield tag='001'>221960</controlfield>
    <datafield tag='084'>
       <subfield code='a'>s035rhe</subfield>
       <subfield code='2'>PDG</subfield>
       <subfield code='9'>PDG</subfield>
    </datafield>
    <datafield tag='084'>
       <subfield code='a'>s035rhm</subfield>
       <subfield code='2'>PDG</subfield>
       <subfield code='9'>PDG</subfield>
    </datafield>
    <datafield tag='084'>
       <subfield code='a'>s035rho</subfield>
       <subfield code='2'>PDG</subfield>
       <subfield code='9'>PDG</subfield> 
    </datafield>
 </record>
"""
#************ GLOBALS ********************
#INSPIRE_URL = 'http://inspirehep.net/search?'
INSPIRE_URL = 'http://inspireheptest.cern.ch/search?'
PDG_FILE    = 'pdg_sm.csv'
  
def parse_fields(row):   
    """
    if row starts with #, journal is returned as an empty string
    
    """
             
    journal = row[0].strip().upper()                
    if journal.startswith('#'): # skip comments
        volume = pages = journal = ''
        codes = []    
    else:
        volume, pages = [x.strip().upper() for x in row[1:3]]
        codes = [x.strip().lower() for x in row[3:]]
        
    return journal, volume, pages, codes
#-----------------------------------------------------------------------------        
      
def get_search_url(journal, volume, pages):
    """
    Get INSPIRE search URL (url-encoded)
    
    If volume or pages is blank, it is assumed an irn of the form    
      http://inspirehep.net/search?p=find%20irn%204034872&of=id
    Otherwise, it's taken as a journal
      http://inspirehep.net/search?p=find%20j%20PHLTA,14,105&of=id
    
    Return the url encoded url with output format set to id
    """
    global INSPIRE_URL
    
    if volume or pages:    
        search_str = 'find j ' + ','.join([journal, volume, pages])
    else:
        search_str = 'find irn ' + journal
        
    return INSPIRE_URL + urllib.urlencode({'p': search_str, 'of': 'id'})
#-----------------------------------------------------------------------------    
              
def get_inspire_id(journal, volume, pages):
    """
    Search in inspirehep.net for record id(s)  based on journal, volume and pages
       
    Return record id list (empty list of none found) and the manipulate flag 
    to notify if volume letter was in an unexpected place. 
    """
    letters = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
    letter = ''    
    manipulate = False
    
    #no need to look for volume letter on pages. PHRVA,D43,R1,s066crd
    def permutations(volume,letter):
        yield letter+volume
        yield volume+letter
        

    search_url = get_search_url(journal, volume, pages)
    #print search_url
    
    #try without manipulation
    hits_handle = urllib2.urlopen(search_url)
    hits = json.loads(hits_handle.read())
    
    if len(hits) == 0:
        #try volume permutations
        manipulate = True            
        if volume:
            if volume[0] in letters:
                letter = volume[0]
                volume = volume[1:]
            elif volume[-1] in letters:
                letter = volume[-1]
                volume = volume[:-1]
            
            if letter != '':
                for vol in permutations(volume,letter):
                    search_url = get_search_url(journal, vol, pages)                      
                    hits_handle = urllib2.urlopen(search_url)
                    hits = json.loads(hits_handle.read())
                    if len(hits) > 0:
                        break       
            
    return hits,manipulate
#-----------------------------------------------------------------------------    

def get_marc_record(record_id, codes):
    """
    create a MARC 21 record using the record id and the PDG codes
    
    """
    
    xml_str = ' <record>\n    <controlfield tag=\'001\'>' + record_id + '</controlfield>'

    for code in codes:
        xml_str = xml_str + '\n    <datafield tag=\'084\'>' + '\n       <subfield code=\'a\'>' + code + \
                                    '</subfield>' + '\n       <subfield code=\'2\'>PDG</subfield>' + \
                                    '\n       <subfield code=\'9\'>PDG</subfield>\n    </datafield>'
    xml_str = xml_str + '\n </record>\n'
    
    
    return xml_str
#-----------------------------------------------------------------------------    

def write_to_file(file_name, save_str):
    """
    Write string to file
    
    """    
    fh = open(file_name, 'wb')            
    fh.write(save_str)           
    fh.close()
        
#-----------------------------------------------------------------------------
             
                
def main():           
    #Initialize    
    DEBUGCOUNT   = 0 
    hits         = []
    manipulate   = False 
    duplicates   = found = not_found = manipulate_count = 0
    xml_str      = '<?xml version="1.0" encoding="UTF-8"?>\n' + '<collection xmlns="http://www.loc.gov/MARC21/slim">\n'  
    manipulated_str = not_found_str = dups_str = '' #to save to a file
    
    print 'processing PDGs...'
    pdg_reader   = csv.reader(open(PDG_FILE, 'rb'), delimiter=',', quotechar='"', skipinitialspace=True)
    
    #main loop        
    for row_str_list in pdg_reader:                          
        journal, volume, pages, codes = parse_fields(row_str_list)
        current_row  = ''            
        #if journal is blank, it's probably a comment
        if journal:                        
            DEBUGCOUNT += 1
            if DEBUGCOUNT % 10 == 0:
                print str(DEBUGCOUNT) + "records processed. Sleeping..."
                time.sleep(random.randint(1, 9))
            current_row =  current_row + journal + ',' + volume + ',' + pages + ',' + ','.join(codes) + '\n'
            print current_row        
            hits, manipulate = get_inspire_id(journal, volume, pages)           
            hits_len = len(hits)
                        
            if hits_len > 0:
                if hits_len > 1:
                    duplicates +=1    
                    dups_str = dups_str + current_row            
                else:
                    #** one found ***
                    found += 1                        
                    xml_str = xml_str + get_marc_record(str(hits[0]), codes)          
            else:
                not_found += 1
                not_found_str = not_found_str + current_row
            
            if manipulate: 
                manipulate_count += 1
                manipulated_str = manipulated_str + current_row
    
    xml_str = xml_str + '</collection>\n</xml>'
    
    #save files    
    write_to_file('marcxml.txt', xml_str)
    write_to_file('needed_a_massage.txt', manipulated_str)
    write_to_file('not_found.txt', not_found_str)
    write_to_file('dups.txt', dups_str)
    
    #reporting...                        
    print 'Found: ' + str(found)     
    print 'Not Found: ' + str(not_found)
    print 'Duplicates: ' + str(duplicates)     
    print 'Manipulated: ' + str(manipulate_count)
    print 'Total: ' + str(DEBUGCOUNT) + '\n'
    print 'done'

if __name__ == '__main__':
    main()
