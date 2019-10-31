# Mark Hayhurst - October 2019

import mysql.connector
from slugify import slugify

HOST = "192.196.156.250"
HOST = "127.0.0.1"

SOURCE_USER = "ocdev_ocadmin"
SOURCE_PASSWORD = "@xxwM39eV54B"
SOURCE_DATABASE = "ocdev_saa_staging"

TARGET_USER = "iamwp_dev"
TARGET_PASSWORD = "J!2hFvaAKIq4"
TARGET_DATABASE = "iamwp_dev"

MY_CACHE = None

def cache():
    """singleton reference to dict"""

    global MY_CACHE

    if MY_CACHE:
        MY_CACHE
    return MY_CACHE


def connect(host, user, password, database):

    db = mysql.connector.connect(
        host=host,
        user=user,
        passwd=password,
        database=database,
    )

    return db


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

    my_cursor.execute(sql, args)

    if commit:
        db.commit()

    if get_last_id:
        return my_cursor.lastrowid
    else:
        return my_cursor.fetchall()


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


def migrate_categories(my_source_db, my_target_db, table, label):

    my_source_cursor = my_source_db.cursor()
    my_target_cursor = my_target_db.cursor()

    # get parent id

    sql = "SELECT term_id FROM wp_terms where name='%s' " % label
    results = execute_sql(my_target_db, sql, '' )
    group_id = results[0][0]
    parent = group_id

    # get source data

    sql = "SELECT * FROM %s ORDER BY ID" % table

    results = execute_sql(my_source_db, sql, '' )

    # create individual rows

    wp_terms_args = dict()
    wp_termmeta_args = dict()
    wp_term_taxonomy_args = dict()

    wp_terms_args['term_group'] = 0
    wp_term_taxonomy_args['taxonomy'] = 'product_cat'
    wp_term_taxonomy_args['parent'] = parent
    wp_term_taxonomy_args['count'] = 0

    for r in results:
        print(r)
        wp_terms_sql = "INSERT INTO wp_terms (name,slug,term_group) VALUES (%(name)s,%(slug)s,%(term_group)s)"
        wp_terms_args['name'] = r[1]
        wp_terms_args['slug'] = slugify(r[1])
        print( wp_terms_args)
        term_id = execute_sql(my_target_db, wp_terms_sql, wp_terms_args, commit=True, get_last_id=True)

        wp_termmeta_args['term_id'] = term_id
        wp_termmeta_args['order'] = 0
        wp_termmeta_args['display_type'] = ''
        wp_termmeta_args['thumbnail_id'] = 0
        for meta_key in ['order', 'display_type', 'thumbnail_id']:
            wp_termmeta_args['meta_key'] = meta_key
            wp_termmeta_args['meta_value'] = wp_termmeta_args[meta_key]
            wp_termmeta_sql = "INSERT INTO wp_termmeta (term_id,meta_key,meta_value) VALUES (%(term_id)s,%(meta_key)s,%(meta_value)s)"
            print( wp_termmeta_args)
            meta_id = execute_sql(my_target_db, wp_termmeta_sql, wp_termmeta_args, commit=True, get_last_id=True)

        wp_term_taxonomy_args['term_id'] = term_id
        wp_term_taxonomy_args['description'] = ''  # needs to be blank
        wp_term_taxonomy_sql = "INSERT INTO wp_term_taxonomy (term_id,taxonomy,description,parent,count) VALUES (%(term_id)s,%(taxonomy)s,%(description)s,%(parent)s,%(count)s)"
        print( wp_term_taxonomy_args)
        term_taxonomy_id = execute_sql(my_target_db, wp_term_taxonomy_sql, wp_term_taxonomy_args, commit=True, get_last_id=True)


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

    r = execute_sql(source_db, 'SELECT * FROM Artists LIMIT 10', '')
    print(r)

    # migrate_categories(source_db, target_db, "WorksCategory", "Category")
    # migrate_categories(source_db, target_db, "WorksSubject", "Subject")
    # migrate_categories(source_db, target_db, "WorksMedium", "Medium")

    pass

main()