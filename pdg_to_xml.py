import csv
import re
import time
import urllib
import urllib2
import sys
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
INSPIRE_URL = 'http://inspirehep.net/search?'
#INSPIRE_URL  = 'http://inspireheptest.cern.ch/search?'
PDG_FILE     = 'pdg_test.csv'
#PDG_FILE     = 'PDGIdentifiers-references-2012v0.txt'
SLEEP_NUMBER = 2
MANUALLY_FOUND = 'manually_found_recids.txt'
  
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
    
def get_hits(journal, volume, pages):
    """
    prepare the search url bases on parameters and execute search
        
    Return hit list or empty list if no record was found
    repetitive code, so putting it in a function
    
    """
        
    search_url = get_search_url(journal, volume, pages)
    print 'search_url ' + search_url    
    hits_handle = urllib2.urlopen(search_url)
    hits = json.loads(hits_handle.read())
    
    return hits
#-----------------------------------------------------------------------------        
    
    
def try_special_cases(journal, volume, pages):
   
    #if record found, return
    v = volume
    p = pages
    
    if journal == 'JPBAB': 
        v = 'B' + volume
    elif journal == 'NUPHZ':
        if volume[0] == 'B':
            v = volume[1:]
    elif journal == 'PRLTA':
        if pages[0] == 'A':
            p = pages[1:]
        elif pages[-1] == 'A':
            p = pages[:-1]
    elif journal == 'PRPLC':
        if volume[0] == 'C':
            v = volume[1:]
        elif volume[-1] == 'C':
            v = volume[:-1]
    elif journal == 'PHRVA':
        if volume[0] == 'B':
            p = 'B' + pages
            v = volume[1:]
        elif volume[-1] == 'B':
            p = 'B' + pages
            v = volume[:-1]
    print 'in special cases: ' + journal + ' v:' + v + ' p: ' + p
     
    return get_hits(journal, v, p)    
    
    
    
def get_inspire_id(journal, volume, pages):
    """
    Search in inspirehep.net for record id(s)  based on journal, volume and pages
       
    When records are not found initially, the script manipulates the volume 
    and/or pages based on the type of journal. This increases the hit rate.
    
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
        
    hits = get_hits(journal, volume, pages)    
    if len(hits) == 0:
        
        #try volume and pages permutations
        manipulate = True        
        
        #try special cases
        hits = try_special_cases(journal, volume, pages)
        
        #if no hit found, check for PHRVA and NUPHA.if found, remove letter from page        
        if len(hits) < 1 and (journal == 'PHRVA' or journal == 'NUPHA'):
            if pages[0] in letters:
                pages = pages[1:]
            elif pages[-1] in letters:
                pages = pages[:-1]
            
            hits = get_hits(journal, volume, pages)
        #moving the volume letter
        if len(hits) < 1 and volume:
            if volume[0] in letters:
                letter = volume[0]
                volume = volume[1:]
            elif volume[-1] in letters:
                letter = volume[-1]
                volume = volume[:-1]
            
            if letter != '':
                for vol in permutations(volume,letter):                    
                    hits = get_hits(journal, vol, pages)
                    if len(hits) > 0:
                        break
            else:
                #no letter was found in volume, so check for JPHGB and JPAGB
                if journal == 'JPHGB':
                    vol = 'G' + volume
                elif journal == 'JPAGB':
                    vol = 'A' + volume  
                else: 
                    vol = volume                                                  
                hits = get_hits(journal, vol, pages)
          
            
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
        
def get_manually_found(journal, volume, pages, manually_found_lines):           
    hits = []
    for line in manually_found_lines:        
        match = re.search(r'(.+)(recid:\s*)(\d*)', line)        
        if match:                      
            recid =  match.group(3)     
            row = match.group(1).split(',')      
            j, v, p, c = parse_fields(row)
            print 'in get_manually_found '  + j + ' ' + v + ' ' + p
            if j == journal and v == volume and p == pages:
                hits.append(recid)           
                 
    return hits
      
                
def main():           
    #Initialize    
    DEBUGCOUNT   = 0 
    hits         = manually_found_lines = []
    manipulate   = False 
    duplicates   = found = not_found = manipulate_count = manually_found_count = 0
    xml_str      = '<?xml version="1.0" encoding="UTF-8"?>\n' + '<collection xmlns="http://www.loc.gov/MARC21/slim">\n'  
    manipulated_str = not_found_str = dups_str = '' #to save to a file
    
    print 'processing PDGs...'
    try:
        pdg_reader   = csv.reader(open(PDG_FILE, 'rb'), delimiter=',', quotechar='"', skipinitialspace=True)
    except:
        print 'Failed to open:' + PDG_FILE
        sys.exit()
    
    #read manually found file
    try:
        f = open(MANUALLY_FOUND)
        manually_found_lines = f.readlines()
        f.close()
    except:    
        print 'Failed to open:Manually_found file.'
        
    #main loop        
    for row_str_list in pdg_reader:            
        hits = []              
        journal, volume, pages, codes = parse_fields(row_str_list)
        current_row  = ''            
        #if journal is blank, it's probably a comment
        if journal:                        
            DEBUGCOUNT += 1
            if DEBUGCOUNT % SLEEP_NUMBER == 0:
                print str(DEBUGCOUNT) + "records processed. Sleeping..."
                #time.sleep(random.randint(1, 9))
                time.sleep(5)
            current_row =  current_row + journal + ',' + volume + ',' + pages + ',' + ','.join(codes) + '\n'
            print '*' + current_row     
                        
            #first, try manually found
            if len(manually_found_lines) > 0:               
                hits = get_manually_found(journal, volume, pages, manually_found_lines)                
                
            if len(hits) > 0:
                print 'manually_found_cound:' + str(manually_found_count) + ' hits[0]: ' + str(hits[0])
                manually_found_count += 1
            else:                   
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
    print 'Manually Found: ' + str(manually_found_count) 
    print 'Total: ' + str(DEBUGCOUNT) + '\n'
    print 'done'

if __name__ == '__main__':
    main()
