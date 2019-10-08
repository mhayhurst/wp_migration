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


def execute_sql(db, sql, args, commit=False, get_last_id=False):

    my_cursor = db.cursor()

    my_cursor.execute(sql)

    if commit:
        db.commit()

    if get_last_id:
        return my_cursor.lastrowid
    else:
        my_result = my_cursor.fetchall()


def create_wp_user(r):

    # delete existing from wp_usermeta

    sql = "DELETE FROM wp_usermeta WHERE user_id in (SELECT ID FROM wp_users WHERE user_login=%s)"
    execute_sql(TARGET_DB, sql, args=r['Code'])

    # delete existing from wp_users

    sql = "DELETE FROM wp_users WHERE user_login=%s"
    execute_sql(TARGET_DB, sql, args=r['Code'])

    # insert new in wp_users

    sql = ("INSERT INTO wp_users"
           " (user_login, user_pass, user_nicename, user_email, user_url, user_registered, user_activation_key, user_status, display_name)"
           "VALUES %(user_login)s, %(user_pass)s, %(user_nicename)s, %(user_url)s, %(user_registered)s, %(user_activation_key)s, %user_status)s, %(display_name)s")

    t = dict()
    t['user_login'] = r["Code"]
    t['user_pass'] = ''
    t['user_nicename'] = r["Code"]
    t['user_email'] = r["EmailAddress"]
    t['user_url'] = ''
    t['user_registered'] = r["Created"]
    t['user_activation_key'] = ''
    t['user_status'] = 0
    t['display_name'] = r["Code"]

    ID = execute_sql(target_db, sql, t, commit=True, get_last_id=True)

    # insert new in wp_usermeta


def migrate_users():

    # delete existing data

    # fetch data

    my_cursor = source_db.cursor()

    sql = "SELECT * FROM Artists LIMIT 100"
    my_cursor.execute(source_db, sql)

    my_result = my_cursor.fetchall()

    for r in my_result:
        create_wp_user(r)
        # insert wp_usermeta(r)
        print(r)


def main():

    source_db = connect(HOST, SOURCE_USER, SOURCE_PASSWORD, SOURCE_DATABASE)
    target_db = connect(HOST, TARGET_USER, TARGET_PASSWORD, TARGET_DATABASE)

    pass

main()