#!/usr/bin/env python3
"""
Module Docstring
Parse cross_references.txt 
  from https://www.openbible.info/labs/cross-references/
"""

__author__ = "John D. Lewis"
__version__ = "0.1.0"
__license__ = "MIT"

import sqlite3
import csv
import logging
# logging.basicConfig(filename='output.log',level=logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)

bookList = ['Gen', 'Exod', 'Lev', 'Num', 'Deut', 'Josh', 'Judg', 'Ruth', '1Sam', '2Sam', '1Kgs', '2Kgs', '1Chr', '2Chr', 'Ezra', 'Neh', 'Esth', 'Job', 'Ps', 'Prov', 'Eccl', 'Song', 'Isa', 'Jer', 'Lam', 'Ezek', 'Dan', 'Hos', 'Joel', 'Amos', 'Obad', 'Jonah', 'Mic', 'Nah', 'Hab', 'Zeph', 'Hag', 'Zech', 'Mal', 'Matt', 'Mark', 'Luke', 'John', 'Acts', 'Rom', '1Cor', '2Cor', 'Gal', 'Eph', 'Phil', 'Col', '1Thess', '2Thess', '1Tim', '2Tim', 'Titus', 'Phlm', 'Heb', 'Jas', '1Pet', '2Pet', '1John', '2John', '3John', 'Jude', 'Rev']

def padTo3(val):
    val = int(val)
    if(val < 10):
        valOut = "00" + str(val)
    elif(val < 100):
        valOut = "0" + str(val)
    else:
        valOut = str(val)
    # logging.debug("padTo3(%s) returning %s" % (val, valOut))
    return(valOut)

def convertToVid(b,c,v):
    bo = str(bookList.index(b)+1)
    co = padTo3(c)
    vo = padTo3(v)
    return(bo + co + vo)

# Output Format
# source_book, source_chp, source_verse, target1_book, target1_chapter, target1_verse, target2_book, target2_chapter, target2_verse, 
# target2* will only be printed when 
def main():
    """ Parse the CSV file """
    BIBLE_BOOK_COUNT = 66

    # conn = sqlite3.connect('cross_references.sqlite3')
    conn = sqlite3.connect('c:/users/balin/code/Bible/bibles-parsed.db')
    c = conn.cursor()

    with open('cross_references.txt', newline='') as csvfile:
    # with open('cross50.txt', newline='') as csvfile:
        crreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        next(csvfile)
        updateList = []

        for row in crreader:
            (f, t, score) = row[0].split("\t")
            score = int(score)
            # Parse From ref
            (fb,fc,fv) = f.split('.')
            from_vid = convertToVid(fb,fc,fv)
            if fb not in bookList:
                bookList.append(fb)
            # Parse To ref
            if "-" in t:
                # logging.info("Found split To ref: %s" % (t))
                (t1, t2) = t.split("-")
                (t1b,t1c,t1v) = t1.split('.')
                (t2b,t2c,t2v) = t2.split('.')
                to1_vid = convertToVid(t1b,t1c,t1v)
                to2_vid = convertToVid(t2b,t2c,t2v)

                if (t1b == t2b):
                    if (t1c == t2c):
                        updateList.append((fb,int(fc),int(fv),t1b,int(t1c),int(t1v), None, None,int(t2v), score, from_vid, to1_vid, to2_vid))
                    else:
                        updateList.append((fb,int(fc),int(fv),t1b,int(t1c),int(t1v), None, int(t2c),int(t2v), score, from_vid, to1_vid, to2_vid))
                else:
                    updateList.append((fb,int(fc),int(fv),t1b,int(t1c),int(t1v),t2b,int(t2c),int(t2v), score, from_vid, to1_vid, to2_vid))
            else: # Not a range
                (tb,tc,tv) = t.split('.')

                to1_vid = convertToVid(tb,tc,tv)
                to2_vid = None

                updateList.append((fb,int(fc),int(fv),tb,int(tc),int(tv), None, None, None, score, from_vid, to1_vid, None))

    if (len(bookList) == BIBLE_BOOK_COUNT):
        logging.info("Working: Covers all %s books" % (BIBLE_BOOK_COUNT)) 
    else:
        logging.warning("Invalid book count: %s" % (len(bookList)))

    # logging.info(updateList)
    c.executemany('INSERT INTO cross_references ("from_book", "from_chp", "from_verse", "to1_book", "to1_chp", "to1_verse", "to2_book", "to2_chp", "to2_verse", "score", "from_vid", "to1_vid", "to2_vid") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', updateList)
    conn.commit()
    logging.info("Done updating DB")
    conn.close()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    main()