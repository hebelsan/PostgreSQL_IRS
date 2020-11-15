from Funcs.create_database import *
from Funcs.ranking import *
from Funcs.add_psql_functions import *


if __name__ == '__main__':
    while(True):
        print('0: exit')
        print('1: create necessary tables')
        print('2: show tables')
        print('3: insert test doc')
        print('4: drop all databases and functions')
        print('5: rank tf-idf')
        print('6: rank bm25')
        print('7: rank tf-idf with word proximity')
        print('8: rank bm25 with word proximity')
        print('10: create function necessary for ranking')
        num = input('')
        if num == '0':
            exit()
        elif num == '1':
            db_create_tables()
        elif num == '2':
            db_show_tables()
        elif num == '3':
            db_insert_test_data()
        elif num == '4':
            db_drop_all_tables()
        elif num == '5':
            print('query could be a single term or multiple connected with &(AND), |(OR), !(NOT), <N>(Followed by)')
            query = input('query: ')
            db_rank_sec_tfidf(query)
        elif num == '6':
            print('query could be a single term or multiple connected with &(AND), |(OR), !(NOT), <N>(Followed by)')
            query = input('query: ')
            db_rank_sec_bm25(query)
        elif num == '7':
            print('query could be a single term or multiple connected with &(AND), |(OR), !(NOT), <N>(Followed by)')
            query = input('query: ')
            db_rank_sec_tfidf_ext(query)
        elif num == '8':
            print('query could be a single term or multiple connected with &(AND), |(OR), !(NOT), <N>(Followed by)')
            query = input('query: ')
            db_rank_sec_bm25_ext(query)
        elif num == '10':
            print('creating functions...')
            add_psql_functions()
        else:
            print('please insert a valid input')
