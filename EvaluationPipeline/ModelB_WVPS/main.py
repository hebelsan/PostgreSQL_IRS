from Funcs.create_database import *
from Funcs.ranking import *
from Funcs.search import db_search


if __name__ == '__main__':
    while(True):
        print('0: exit')
        print('1: create necessary tables')
        print('2: drop all databases and functions')
        print('3: show tables')
        print('4: insert test doc')
        print('5: search')
        print('6: rank tf-idf')
        print('7: rank bm25')
        print('8: rank tf-idf extended')
        print('9: rank bm25 extended')
        num = input('')
        if num == '0':
            exit()
        elif num == '1':
            db_create_tables()
        elif num == '2':
            db_drop_all_tables()
        elif num == '3':
            db_show_tables()
        elif num == '4':
            db_insert_test_data()
        elif num == '5':
            print('query could be a single term or multiple connected with AND, OR. If different operators are mixed together use round brackets!!')
            query = input('query: ')
            db_search(query)
        elif num == '6':
            print('connect query terms with AND, OR (If different operators are mixed together use round brackets!)')
            query = input('query: ')
            db_rank_sec_tfidf(query)
        elif num == '7':
            print('connect query terms with AND, OR (If different operators are mixed together use round brackets!)')
            query = input('query: ')
            db_rank_sec_bm25(query)
        elif num == '8':
            print('connect query terms with AND, OR (If different operators are mixed together use round brackets!)')
            query = input('query: ')
            db_rank_sec_tfidf_ext(query)
        elif num == '9':
            print('connect query terms with AND, OR (If different operators are mixed together use round brackets!)')
            query = input('query: ')
            db_rank_sec_bm25_ext(query)
        else:
            print('please insert a valid input')
