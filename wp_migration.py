# Mark Hayhurst - October 2019

import mysql.connector
from slugify import slugify
import datetime
import time

import wp_crypt

HOST = "192.196.156.250"
HOST = "127.0.0.1"

SOURCE_USER = "ocdev_ocadmin"
SOURCE_PASSWORD = "@xxwM39eV54B"
SOURCE_DATABASE = "ocdev_saa_staging"

TARGET_USER = "iamwp_dev"
TARGET_PASSWORD = "J!2hFvaAKIq4"
TARGET_DATABASE = "iamwp_dev"

SOURCE_DB = None
TARGET_DB = None

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


def execute_sql(db, sql, args, commit=False, get_results=True, get_last_id=False):

    my_cursor = db.cursor(dictionary=True)

    # print(sql)
    # print(args)
    # if args:
    #     print(sql % args)

    my_cursor.execute(sql, args)

    if commit:
        db.commit()

    if get_last_id:
        return my_cursor.lastrowid
    elif get_results:
        return my_cursor.fetchall()


def get_acf_field_code(field_name):

    global TARGET_DB

    args = {"post_excerpt": field_name}
    sql = "SELECT post_name FROM wp_posts WHERE post_excerpt=%(post_excerpt)s"
    results = execute_sql(TARGET_DB, sql, args, False, get_results=True)
    return results[0]['post_name']


def xstr(s):
    return '' if s is None else str(s)


def create_user(r):

    global TARGET_DB
    t = dict()

    print(r['Code'])

    t['user_login'] = r['Code']

    # delete existing from wp_usermeta

    sql = "DELETE FROM wp_usermeta WHERE user_id in (SELECT ID FROM wp_users WHERE user_login=%(user_login)s)"
    execute_sql(TARGET_DB, sql, t, commit=True, get_results=False)

    # delete existing from wp_users

    sql = "DELETE FROM wp_users WHERE user_login=%(user_login)s"
    execute_sql(TARGET_DB, sql, t, commit=True, get_results=False)

    # insert new in wp_users

    sql = ("INSERT INTO wp_users"
           " (user_login, user_pass, user_nicename, user_email, user_url, user_registered, user_activation_key, user_status, display_name)"
           " VALUES(%(user_login)s, %(user_pass)s, %(user_nicename)s, %(user_email)s, %(user_url)s, %(user_registered)s, %(user_activation_key)s, %(user_status)s, %(display_name)s)")

    t['user_login'] = r["Code"]
    # password = "PASSWORD".encode('utf-8')
    # t['user_pass'] = password # wp_crypt.crypt_private(r['Password'].encode('utf-8'))
    t['user_pass'] = '' # TODO: sort out password generation
    t['user_nicename'] = r["Code"]
    t['user_email'] = r['Code'] + "@ubiservers.net"
    t['user_url'] = xstr(r['WebAddress'])
    t['user_registered'] = str(r["Created"])
    t['user_activation_key'] = ''
    t['user_status'] = 0
    t['display_name'] = r["Code"]

    USER_ID = execute_sql(TARGET_DB, sql, t, commit=True, get_last_id=True)

    # insert new in wp_usermeta

    wp_user_meta_rows = dict()

    wp_user_meta_rows['nickname'] = r['Code']
    wp_user_meta_rows['first_name'] = r['FirstNames']
    wp_user_meta_rows['billing_first_name'] = r['FirstNames']
    wp_user_meta_rows['shipping_first_name'] = r['FirstNames']
    wp_user_meta_rows['last_name'] = r['FamilyName']
    wp_user_meta_rows['billing_last_name'] = r['FamilyName']
    wp_user_meta_rows['shipping_last_name'] = r['FamilyName']
    wp_user_meta_rows['description'] = ''
    wp_user_meta_rows['rich_editing'] = 'true'
    wp_user_meta_rows['syntax_highlighting'] = 'true'
    wp_user_meta_rows['comment_shortcuts'] = 'false'
    wp_user_meta_rows['admin_color'] = 'fresh'
    wp_user_meta_rows['use_ssl'] = '0'
    wp_user_meta_rows['show_admin_bar_front'] = 'true'
    wp_user_meta_rows['locale'] = ''
    wp_user_meta_rows['wp_capabilities'] = 'a:2:{s:6:"author";b:1;s:6:"artist";b:1;}'
    wp_user_meta_rows['wp_user_level'] = '2'
    wp_user_meta_rows['dismissed_wp_pointers'] = ''

    wp_user_meta_rows['billing_company'] = r['Organisation']
    wp_user_meta_rows['shipping_company'] = r['Organisation']

    wp_user_meta_rows['billing_address_1'] = r['PostalAddress']
    wp_user_meta_rows['billing_address_2'] = ''
    wp_user_meta_rows['billing_city'] = r['PostalTown']
    wp_user_meta_rows['billing_postcode'] = r['PostalCode']
    wp_user_meta_rows['billing_state'] = '' # TODO: lookup state code
    wp_user_meta_rows['billing_country'] = 'ZA' # TODO: lookup country code

    wp_user_meta_rows['shipping_address_1'] = r['StreetAddress1']
    wp_user_meta_rows['shipping_address_2'] = r['StreetAddress2']
    wp_user_meta_rows['shipping_city'] = r['Town']
    wp_user_meta_rows['shipping_postcode'] = r['PostCode']
    wp_user_meta_rows['shipping_state'] = '' # TODO: lookup state code
    wp_user_meta_rows['shipping_country'] = 'ZA'

    wp_user_meta_rows['description'] = r['Exhibitions']

    wp_user_meta_rows['artist_id'] = r['ID']
    wp_user_meta_rows['_artist_id'] = get_acf_field_code('artist_id')

    wp_user_meta_rows['artist_mobile_phone'] = r['MobilePhone']
    wp_user_meta_rows['_artist_mobile_phone'] = get_acf_field_code('artist_mobile_phone')

    wp_user_meta_rows['artist_work_phone'] = r['WorkPhone']
    wp_user_meta_rows['_artist_work_phone'] = get_acf_field_code('artist_work_phone')

    wp_user_meta_rows['artist_fax'] = r['Fax']
    wp_user_meta_rows['_artist_fax'] = get_acf_field_code('artist_fax')

    wp_user_meta_rows['artist_region'] = r['Region']
    wp_user_meta_rows['_artist_region'] = get_acf_field_code('artist_region')

    wp_user_meta_rows['artist_artist_notes'] = r['ArtistNotes']
    wp_user_meta_rows['_artist_artist_notes'] = get_acf_field_code('artist_artist_notes')

    wp_user_meta_rows['artist_education'] = r['Education']
    wp_user_meta_rows['_artist_education'] = get_acf_field_code('artist_education')

    wp_user_meta_rows['artist_preferred_media'] = r['PreferredMedia']
    wp_user_meta_rows['_artist_preferred_media'] = get_acf_field_code('artist_preferred_media')

    wp_user_meta_rows['artist_promotion_authorised'] = r['PromotionAuthorised']
    wp_user_meta_rows['_artist_promotion_authorised'] = get_acf_field_code('artist_promotion_authorised')

    wp_user_meta_rows['artist_vat_registered'] = r['VATRegistered']
    wp_user_meta_rows['_artist_vat_registered'] = get_acf_field_code('artist_vat_registered')

    wp_user_meta_rows['artist_deceased'] = r['Deceased']
    wp_user_meta_rows['_artist_deceased'] = get_acf_field_code('artist_deceased')

    wp_user_meta_rows['artist_accepts_commissions'] = r['AcceptsCommissions']
    wp_user_meta_rows['_artist_accepts_commissions'] = get_acf_field_code('artist_accepts_commissions')

    wp_user_meta_rows['artist_seo_slug'] = r['SEOFriendly']
    wp_user_meta_rows['_artist_seo_slug'] = get_acf_field_code('artist_seo_slug')

    wp_user_meta_rows['artist_editor_notes'] = r['EditorNotes']
    wp_user_meta_rows['_artist_editor_notes'] = get_acf_field_code('artist_editor_notes')

    wp_user_meta_rows['_order_count'] = '0'
    wp_user_meta_rows['last_update'] = str(int(time.mktime(datetime.datetime.now().timetuple())))

    for c in wp_user_meta_rows:
        t['user_id'] = USER_ID
        t['meta_key'] = c
        t['meta_value'] = wp_user_meta_rows[c]

        sql = ("INSERT INTO wp_usermeta"
               " (user_id, meta_key, meta_value)"
               "VALUES(%(user_id)s, %(meta_key)s, %(meta_value)s)")
        UMETA_ID = execute_sql(TARGET_DB, sql, t, commit=True, get_last_id=True)


def migrate_products():
    global SOURCE_DB
    global TARGET_DB

    my_source_cursor = SOURCE_DB.cursor()
    my_target_cursor = TARGET_DB.cursor()

    # get term_ids

    sql = "SELECT term_id, name FROM wp_terms"
    results = execute_sql(TARGET_DB, sql, '')

    term_ids = dict()

    for r in results:
        term_ids[r['name']] = r['term_id']

    # get current lookups from source

    saa_lookups = dict()
    saa_lookups['Category'] = dict()
    saa_lookups['Subject'] = dict()
    saa_lookups['Medium'] = dict()

    sql = "SELECT ID, Description FROM WorksCategory"
    results = execute_sql(SOURCE_DB, sql, '')

    for r in results:
        saa_lookups['Category'][r['ID']] = r['Description']

    sql = "SELECT ID, Description FROM WorksSubject"
    results = execute_sql(SOURCE_DB, sql, '')

    for r in results:
        saa_lookups['Subject'][r['ID']] = r['Description']

    sql = "SELECT ID, Description FROM WorksMedium"
    results = execute_sql(SOURCE_DB, sql, '')

    for r in results:
        saa_lookups['Medium'][r['ID']] = r['Description']

    # get source data

    sql = "SELECT * FROM Works ORDER BY ID LIMIT 10"

    results = execute_sql(SOURCE_DB, sql, '')

    # wp_posts

    wp_posts_sql = """
    INSERT INTO
        wp_posts(
            post_author,
            post_date,
            post_date_gmt,
            post_content,
            post_title,
            post_excerpt,
            post_status,
            comment_status,
            ping_status,
            post_password,
            post_name,
            to_ping,
            pinged,
            post_modified,
            post_modified_gmt,
            post_content_filtered,
            post_parent,
            guid,
            menu_order,
            post_type,
            post_mime_type,
            comment_count
        )
    VALUES(
            %(post_author)s,
            %(post_date)s,
            %(post_date_gmt)s,
            %(post_content)s,
            %(post_title)s,
            %(post_excerpt)s,
            %(post_status)s,
            %(comment_status)s,
            %(ping_status)s,
            %(post_password)s,
            %(post_name)s,
            %(to_ping)s,
            %(pinged)s,
            %(post_modified)s,
            %(post_modified_gmt)s,
            %(post_content_filtered)s,
            %(post_parent)s,
            %(guid)s,
            %(menu_order)s,
            %(post_type)s,
            %(post_mime_type)s
        );
    """

    wp_postmeta_sql = """
    INSERT INTO wp_postmeta
        (
            meta_id,
            post_id,
            meta_key,
            meta_value
        )
        VALUES
        (
            %(meta_id)s,
            %(post_id)s,
            %(meta_key)s,
            %(meta_value)s
        );
    """

    wp_term_relationship_sql = """
    INSERT INTO wp_term_relationships
        (
            object_id,
            term_taxonomy_id,
            term_order
        )
        VALUES
        (
            %(object_id)s,
            %(term_taxonomy_id)s,
            %(term_order)s
        );
    """

    wp_wc_product_meta_lookup_sql = """
    INSERT INTO wp_wc_product_meta_lookup
        (
            product_id,
            sku,
            virtual,
            downloadable,
            min_price,
            max_price,
            onsale,
            stock_quantity,
            stock_status,
            rating_count,
            average_rating,
            total_sales
        )
        VALUES
        (
            %(product_id)s,
            %(sku)s,
            %(virtual)s,
            %(downloadable)s,
            %(min_price)s,
            %(max_price)s,
            %(onsale)s,
            %(stock_quantity)s,
            %(stock_status)s,
            %(rating_count)s,
            %(average_rating)s,
            %(total_sales)s
        );
    """

    wp_posts_args = dict()
    wp_postmeta_args = dict()
    wp_term_relationship_args = dict()

    wp_posts_args['post_status'] = 'publish'
    wp_posts_args['post_password'] = None
    wp_posts_args['ping_status'] = 'closed'
    wp_posts_args['common_status'] = 'closed'
    wp_posts_args['to_ping'] = None
    wp_posts_args['pinged'] = None
    wp_posts_args['post_content_filtered'] = None
    wp_posts_args['post_parent'] = 0
    wp_posts_args['menu_order'] = 0
    wp_posts_args['post_type'] = 'product'
    wp_posts_args['comment_count'] = 0

    wp_postmeta_args['total_sales'] = 0
    wp_postmeta_args['_tax_status'] = 'taxable'
    wp_postmeta_args['_tax_class'] = ''
    wp_postmeta_args['_manage_stock'] = 'no'
    wp_postmeta_args['_backorders'] = 'no'
    wp_postmeta_args['_sold_individually'] = 'yes'
    wp_postmeta_args['_virtual'] = 'no'
    wp_postmeta_args['_downloadable'] = 'no'
    wp_postmeta_args['_download_limit'] = -1
    wp_postmeta_args['_download_expiry'] = -1
    wp_postmeta_args['_stock'] = None
    wp_postmeta_args['_wc_average_rating'] = 0
    wp_postmeta_args['_wc_review_count'] = 0
    wp_postmeta_args['_product_version'] = '3.8.0' # ???

    # wp_term_relationship_args['term_taxonomy'] = 2

    for r in results:
        wp_posts_args['post_name'] = r['CanonicalPath']
        wp_posts_args['post_title'] = r['Title']
        wp_posts_args['post_content'] = r['ArtistNotes']
        wp_posts_args['post_date'] = r['Created']
        wp_posts_args['post_date_gmt'] = r['Created']
        wp_posts_args['post_modified'] = r['LastUpdated']
        wp_posts_args['post_modified_gmt'] = r['LastUpdated']
        wp_posts_args['guid'] = None

        print(wp_posts_args)
        post_id = execute_sql(TARGET_DB, wp_posts_sql, wp_posts_args, commit=True, get_last_id=True)

        wp_postmeta_args['post_id'] = post_id

        wp_postmeta_args['_sku'] = r['ID']
        wp_postmeta_args['_stock_status'] = term_ids['instock'] # TODO: Allow for outofstock
        wp_postmeta_args['_regular_price'] = r['Price']
        wp_postmeta_args['_price'] = r['Price']
        wp_postmeta_args['_width'] = r['SizeWidth']
        wp_postmeta_args['_height'] = r['SizeHeight']
        wp_postmeta_args['_length'] = r['SizeDepth']
        wp_postmeta_args['_weight'] = r['Weight']
        # wp_postmeta_args['x'] = None
        # wp_postmeta_args['x'] = None
        # wp_postmeta_args['x'] = None

        print(wp_postmeta_args)
        postmeta_id = execute_sql(TARGET_DB, wp_postmeta_sql, wp_postmeta_args, commit=True, get_last_id=True)

        wp_term_relationship_args['object_id'] = post_id
        wp_term_relationship_args['term_order'] = 0

        # Category

        wp_term_relationship_args['term_taxonomy'] = saa_lookups['Category'][r['CategoryID']]

        print(wp_term_relationship_args)
        execute_sql(TARGET_DB, wp_term_relationship_sql, wp_term_relationship_args, commit=True, get_last_id=True)

        # Subject

        wp_term_relationship_args['term_taxonomy'] = saa_lookups['Subject'][r['SubjectID']]

        print(wp_term_relationship_args)
        execute_sql(TARGET_DB, wp_term_relationship_sql, wp_term_relationship_args, commit=True, get_last_id=True)

        # Medium

        wp_term_relationship_args['term_taxonomy'] = saa_lookups['Medium'][r['MediumID']]

        print(wp_term_relationship_args)
        execute_sql(TARGET_DB, wp_term_relationship_sql, wp_term_relationship_args, commit=True, get_last_id=True)


def migrate_categories(table, label):
    global SOURCE_DB
    global TARGET_DB

    my_source_cursor = SOURCE_DB.cursor()
    my_target_cursor = TARGET_DB.cursor()

    # get parent id

    sql = "SELECT term_id FROM wp_terms where name='%s' " % label
    results = execute_sql(TARGET_DB, sql, '')
    group_id = results[0]['term_id']
    parent = group_id

    # get source data

    sql = "SELECT ID,Description,Sequence FROM %s ORDER BY ID" % table

    results = execute_sql(SOURCE_DB, sql, '')

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
        wp_terms_args['name'] = r['Description']
        wp_terms_args['slug'] = slugify(r['Description'])
        print(wp_terms_args)
        term_id = execute_sql(TARGET_DB, wp_terms_sql, wp_terms_args, commit=True, get_last_id=True)

        wp_termmeta_args['term_id'] = term_id
        wp_termmeta_args['order'] = r['Sequence']
        wp_termmeta_args['display_type'] = ''
        wp_termmeta_args['thumbnail_id'] = 0
        for meta_key in ['order', 'display_type', 'thumbnail_id']:
            wp_termmeta_args['meta_key'] = meta_key
            wp_termmeta_args['meta_value'] = wp_termmeta_args[meta_key]
            wp_termmeta_sql = "INSERT INTO wp_termmeta (term_id,meta_key,meta_value) VALUES (%(term_id)s,%(meta_key)s,%(meta_value)s)"
            print(wp_termmeta_args)
            meta_id = execute_sql(TARGET_DB, wp_termmeta_sql, wp_termmeta_args, commit=True, get_last_id=True)

        wp_term_taxonomy_args['term_id'] = term_id
        wp_term_taxonomy_args['description'] = ''  # needs to be blank
        wp_term_taxonomy_sql = "INSERT INTO wp_term_taxonomy (term_id,taxonomy,description,parent,count) VALUES (%(term_id)s,%(taxonomy)s,%(description)s,%(parent)s,%(count)s)"
        print(wp_term_taxonomy_args)
        term_taxonomy_id = execute_sql(TARGET_DB, wp_term_taxonomy_sql, wp_term_taxonomy_args, commit=True,
                                       get_last_id=True)


def migrate_users():

    global SOURCE_DB

    # delete existing data

    # fetch data

    sql = "SELECT * FROM Artists ORDER BY Code LIMIT 10"
    my_results = execute_sql(SOURCE_DB, 'SELECT * FROM Artists ORDER BY Code', '')

    for r in my_results:
        create_user(r)
        pass


def initial_values():

    pass


def main():

    global SOURCE_DB
    global TARGET_DB

    SOURCE_DB = connect(HOST, SOURCE_USER, SOURCE_PASSWORD, SOURCE_DATABASE)
    TARGET_DB = connect(HOST, TARGET_USER, TARGET_PASSWORD, TARGET_DATABASE)

    # r = execute_sql(SOURCE_DB, 'SELECT * FROM Artists LIMIT 10', '')
    # print(r)

    # migrate_users()

    # migrate_categories("WorksCategory", "Category")
    # migrate_categories("WorksSubject", "Subject")
    # migrate_categories("WorksMedium", "Medium")

    migrate_products()

    pass

main()