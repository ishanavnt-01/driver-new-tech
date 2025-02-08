from django.db import connection


class GenerateViewsetQuery(object):
    def generate_query_sql(self, request):
        qset = self.get_queryset()
        # apply filters
        # get sql for the query that should be run
        for backend in list(self.filter_backends):
            qset = backend().filter_queryset(request, qset, self)
        cursor = connection.cursor().cursor
        sql, params = qset.query.sql_with_params()
        # get properly escaped string representation of the query
        query_str = cursor.mogrify(sql, params)
############### New Added ##############
        try:
            if '' in params:
                name = str("%" + params.index('') + "%")
                data_query = query_str.decode('utf-8')
                mylist = data_query.rsplit("AND", 2)
                mynew_str = '"grout_record"."location_text" LIKE ' + name
                final_query = mylist[0] + " AND " + mynew_str + " AND "+mylist[2]
                cursor.close()
                return final_query
            else:
                cursor.close()
                return query_str.decode('utf-8')
############### End of New Added ##############
        except:
            cursor.close()
            return query_str.decode('utf-8')
