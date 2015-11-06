
## This script parses JSON-formatted Twitter messages (tweets) plus their metadata(i.e. data produced by the Twitter status tracking API).
## Specific entities or attributes are needed so this script solely focuses on those entities. They include 'created_at', 'tweet_id_string',
## 'tweet text', 'user_id_string', 'location of user's account', 'followers_count', 'utc_offset', 'time_zone of the tweet',
## 'coordinates of the tweet location', 'place' of the tweet, 'retweet_count', and 'favorite_count'.
## For more information about the following entities, visit: https://dev.twitter.com/docs.
## Authors: Schuermann, Ryan Thomas <rts@txstate.edu> and Dede-Bamfo, Nathaniel <nd1115@txstate.edu> - November 12, 2013.

# Import all necessary modules
import sys, os, csv, string, timeit, json
from pprint import pprint
#import simplejson as json

# Timer
begin_time = timeit.default_timer()

# Worskpace or Pathname
path = r"C:\Chow_Nat\Twitter_Project\Streamings\BoulderFlood\collected_tweets_boulderfloods"

# Find and specify a file to be parsed
x = filter(lambda x: x.endswith('.json'), os.listdir(path))

tot_processed = 0
not_processed = 0
y = 0

# Loop to find and process all .json files in directory
for j in x:
    outfile = j[0:-4]
    print "Processing {0}...".format(outfile) # Tell me which file is being processed

    # Create a csv file with headers
    fullpath = path + '\\' + outfile + "csv"
    outcsv  = open(fullpath,"wb")
    outfile = csv.writer(outcsv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    outfile.writerow([['created_at'],['tweet_id_str'],['text'],['user_id_str'],['location'],['followers_count'],['utc_offset'],['time_zone'],\
                     ['coordinates'],['place'],['retweet_count'],['favorite_count'],['RT']])

    # Introduce a counter to track how many json files have been identified
    y+=1

    # Find the json file and read it line by line
    json_data = open(j).readlines()
    k = 0

    # A loop to iterate the process of finding and parsing the json files
    for j in json_data:
        k += 1
        # Remove extra lines separating each tweet in the json file
        jsub = j.rstrip('\n').rstrip().strip()
        k+=1
        if (jsub):
            try:
                data = json.loads(j) # Load the read tweets from the json file

                # Some tweets are not retweets so find the retweets and assign a value of 1 in the designated column or field
                if "retweeted_status" in data:
                    rtc = data["retweeted_status"]["retweet_count"]
                    fc = data["retweeted_status"]["favorite_count"]
                    rt = 1
                else: # If not a retweet, just assign a zero as such
                    rtc = fc = rt = 0

                # Start writing the parsed results under their appropriate designated fields or columns in the csv file initially created  
                jdata = [data["created_at"],data["id_str"],filter(lambda x: x in string.printable, data["text"]),data["user"]["id_str"],\
                         filter(lambda x: x in string.printable, data["user"]["location"]),data["user"]["followers_count"],\
                         data["user"]["utc_offset"],data["user"]["time_zone"],data["coordinates"],data["place"],rtc,fc,rt]

                del rt; del rtc; del fc     # Delete all the new variables introduced

                outfile.writerow(jdata)     # Write the output
                tot_processed += 1

            # If there are any inherent errors, please catch them and tell me specifically what they are
            except KeyError, e:
                print e
                not_processed += 1
                #print "{0} {1} \n.....{2}.....\n\n\n\n\n".format(y,k,j)
            except NameError, e:
                print e
                not_processed += 1
                #print "{0} {1} \n.....{2}.....\n\n\n\n\n".format(y,k,j)
            except UnicodeEncodeError, e:
                print e
                not_processed += 1
                #print "{0} {1} \n.....{2}.....\n\n\n\n\n".format(y,k,j)
            except:
                e = sys.exc_info()[0]
                print "Error: %s" % e
                not_processed += 1
                #print "{0} {1} \n.....{2}.....\n\n\n\n\n".format(y,k,j)

#Delete all opened files in memory in this session to unlock them               
    del outcsv; del outfile
del json_data; del j; del k; del y

# Tell me how many I got right and those I missed.
print "Processed {0} Tweets, Not Processed: {1} !!".format(tot_processed,not_processed)

# Timer; tell me how long it took to process everything
end_time = timeit.default_timer()
print "End Processing: {0} seconds".format(end_time - begin_time)


