import mysql.connector

HOST = "192.196.156.250"

SOURCE_USER = "ocdev_ocadmin"
SOURCE_PASSWORD = "@xxwM39eV54B"
SOURCE_DATABASE = "ocdev_saa_staging"

TARGET_USER = "iamwp_dev"
TARGET_PASSWORD = "J!2hFvaAKIq4"
TARGET_DATABASE = "iamwp_dev"


def connect(host, user, password, database):

    db = mysql.connector.connect(
        host=host,
        user=user,
        passwd=password,
    )

    return db


def migrate_WorksCategory(my_source_db, my_target_db):

    my_source_cursor = my_source_db.cursor()
    my_target_cursor = my_target_db.cursor()

    term_id_minimum = 100
    term_id_maximum = 199

    # delete existing

    wp_terms_delete_sql = "DELETE FROM wp_terms WHERE term_id BETWEEN %(term_id_minimum)s AND %(term_id_maximum)"
    wp_term_taxonomy_delete_sql = "DELETE FROM wp_term_taxonomy WHERE term_id BETWEEN %(term_id_minimum)s AND %(term_id_maximum)"

    my_target_cursor.execute(wp_terms_delete_sql, {'term_id_minimum': term_id_minimum, 'term_id_maximum': term_id_maximum})
    my_target_cursor.execute(wp_term_taxonomy_delete_sql, {'term_id_minimum': term_id_minimum, 'term_id_maximum': term_id_maximum})

    # fetch data

    WorksCategory_sql = "SELECT ID,Description,Sequence,PageTitle,MetaDescription,ContentHeading,DescriptiveText,CanonicalURL,LastUpdated FROM WorksCategory ORDER BY ID"
    my_source_cursor.execute(WorksCategory_sql)

    my_result = my_source_cursor.fetchall()

    wp_terms_insert_sql = "INSERT INTO wp_terms(term_id,name,slug,term_group) VALUES(%(term_id)s,%(name)s,%(slug)s,%(term_group)s)"
    wp_term_taxonomy_insert_sql = "INSERT INTO wp_term_taxonomy(term_taxonomy_id,term_id,taxonomy,description,parent,count) VALUES(%(term_taxonomy_id)s,%(term_id)s,%(taxonomy)s,%(description)s,%(parent)s,%(count)s)"

    data_wp_terms = dict()
    data_wp_term_taxonomy = dict()

    term_group = 0
    term_id = 100
    taxonomy = 'product_cat'
    parent = 0
    count = 0

    for r in my_result:
        data_wp_terms['term_id'] = term_id
        data_wp_terms['name'] = r['Name']
        data_wp_terms['slug'] = r['Name']
        data_wp_terms['term_group'] = term_group

        my_target_cursor.execute(wp_terms_insert_sql, data_wp_terms)

        data_wp_term_taxonomy['term_taxonomy_id'] = term_id
        data_wp_term_taxonomy['term_id'] = term_id
        data_wp_term_taxonomy['taxonomy'] = taxonomy
        data_wp_term_taxonomy['description'] = data_wp_terms['name']
        data_wp_term_taxonomy['parent'] = parent
        data_wp_term_taxonomy['count'] = count

        my_target_cursor.execute(wp_term_taxonomy_insert_sql, data_wp_term_taxonomy)

        print(data_wp_terms['name'])

        term_id += 1

    my_target_db.commit()

    my_target_cursor.close()
    my_source_cursor.close()


def migrate_users(source_db, target_db):

    # delete existing data

    # fetch data

    my_cursor = source_db.cursor()

    sql = "SELECT * FROM Artists LIMIT 100"
    my_cursor.execute(sql)

    my_result = my_cursor.fetchall()

    for r in my_result:
        print(r)

    # insert wp_users


def main():

    source_db = connect(HOST, SOURCE_USER, SOURCE_PASSWORD, SOURCE_DATABASE)
    target_db = connect(HOST, TARGET_USER, TARGET_PASSWORD, TARGET_DATABASE)

    pass

main()