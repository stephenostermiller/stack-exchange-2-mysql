import mysql.connector
from lib import localConfig
import re

dbConf = localConfig.getDict('mysql')
cnx = mysql.connector.connect(
    user=dbConf['user'],
    password=dbConf['password'],
    host=dbConf['host'],
    database=dbConf['database'],
    port=dbConf['port'],
    auth_plugin=dbConf['auth_plugin']
)

# Stack Exchange schema documentation
# https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede

tableOptions="ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMPRESSION='ZLIB';"

cnx.cursor().execute(
    "CREATE TABLE IF NOT EXISTS Sites ("
    "  Id SMALLINT PRIMARY KEY NOT NULL,"
    "  ParentId BIGINT,"
    "  TinyName VARCHAR(63) NOT NULL,"
    "  Name VARCHAR(127) NOT NULL,"
    "  LongName VARCHAR(255),"
    "  Url VARCHAR(255) NOT NULL,"
    "  ImageUrl VARCHAR(255),"
    "  IconUrl VARCHAR(255),"
    "  DatabaseName VARCHAR(63),"
    "  Tagline VARCHAR(511),"
    "  TagCss VARCHAR(1023),"
    "  ODataEndpoint VARCHAR(255),"
    "  BadgeIconUrl VARCHAR(255),"
    "  ImageBackgroundColor VARCHAR(255),"
    "  TotalQuestions BIGINT,"
    "  TotalAnswers BIGINT,"
    "  TotalUsers BIGINT,"
    "  TotalComments BIGINT,"
    "  TotalTags BIGINT,"
    "  LastPost DATETIME"
    ") " + tableOptions
)

def createTablesWithSiteId():

    siteEnumVals = "'" + "','".join(querySites()) + "'"
    updateSiteEnums = "ALTER TABLE %s MODIFY COLUMN SiteId ENUM ("+siteEnumVals+") NOT NULL, ALGORITHM=INPLACE, LOCK=NONE;"
    contentLicenseVals="'CC BY-SA 2.5','CC BY-SA 3.0','CC BY-SA 4.0'"

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Badges ("
        "  SiteId ENUM ("+siteEnumVals+") NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  UserId BIGINT NOT NULL,"
        "  Name VARCHAR(255) NOT NULL,"
        "  Date DATETIME NOT NULL,"
        "  Class BIGINT NOT NULL,"
        "  TagBased BOOLEAN NOT NULL,"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, UserId)"
        ") " + tableOptions
    )
    cnx.cursor().execute(updateSiteEnums % "Badges")

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Comments ("
        "  SiteId ENUM ("+siteEnumVals+") NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  PostId BIGINT NOT NULL,"
        "  Score BIGINT NOT NULL,"
        "  Text VARCHAR(2055) NOT NULL,"
        "  CreationDate DATETIME NOT NULL,"
        "  UserId BIGINT,"
        "  UserDisplayName VARCHAR(255),"
        "  ContentLicense ENUM ("+contentLicenseVals+") NOT NULL,"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, PostId)"
        ") " + tableOptions
    )
    cnx.cursor().execute(updateSiteEnums % "Comments")

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS PostHistory ("
        "  SiteId ENUM ("+siteEnumVals+") NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  PostHistoryTypeId ENUM("
        "    'Initial Title',"
        "    'Initial Body',"
        "    'Initial Tags',"
        "    'Edit Title',"
        "    'Edit Body',"
        "    'Edit Tags',"
        "    'Rollback Title',"
        "    'Rollback Body',"
        "    'Rollback Tags',"
        "    'Post Closed',"
        "    'Post Reopened',"
        "    'Post Deleted',"
        "    'Post Undeleted',"
        "    'Post Locked',"
        "    'Post Unlocked',"
        "    'Community Owned',"
        "    'Post Migrated',"
        "    'Question Merged',"
        "    'Question Protected',"
        "    'Question Unprotected',"
        "    'Post Disassociated',"
        "    'Question Unmerged',"
        "    '23',"
        "    'Suggested Edit Applied',"
        "    'Post Tweeted',"
        "    '26',"
        "    '27',"
        "    '28',"
        "    '29',"
        "    '30',"
        "    'Comment discussion moved to chat',"
        "    '32',"
        "    'Post notice added ',"
        "    'Post notice removed',"
        "    'Post migrated away',"
        "    'Post migrated here',"
        "    'Post merge source',"
        "    'Post merge destination',"
        "    '39',"
        "    '40',"
        "    '41',"
        "    '42',"
        "    '43',"
        "    '44',"
        "    '45',"
        "    '46',"
        "    '47',"
        "    '48',"
        "    '49',"
        "    'Bumped by Community User',"
        "    '51',"
        "    'Question became hot',"
        "    'Question removed from hot'"
        "  ) NOT NULL,"
        "  PostId BIGINT NOT NULL,"
        "  RevisionGUID VARCHAR(63) NOT NULL,"
        "  CreationDate DATETIME NOT NULL,"
        "  UserId BIGINT,"
        "  UserDisplayName VARCHAR(255),"
        "  Comment VARCHAR(1023),"
        "  Text MEDIUMBLOB,"
        "  ContentLicense ENUM ("+contentLicenseVals+") NOT NULL,"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, PostId)"
        ") " + tableOptions
    )
    cnx.cursor().execute(updateSiteEnums % "PostHistory")

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS PostLinks ("
        "  SiteId ENUM ("+siteEnumVals+") NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  CreationDate DATETIME NOT NULL,"
        "  PostId BIGINT NOT NULL,"
        "  RelatedPostId BIGINT NOT NULL,"
        "  LinkTypeId ENUM("
        "    'Linked',"
        "    '2',"
        "    'Duplicate'"
        "  ) NOT NULL,"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, PostId)"
        ") " + tableOptions
    )
    cnx.cursor().execute(updateSiteEnums % "PostLinks")

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Posts ("
        "  SiteId ENUM ("+siteEnumVals+") NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  PostTypeId ENUM("
        "    'Question',"
        "    'Answer',"
        "    'Orphaned tag wiki',"
        "    'Tag wiki excerpt',"
        "    'Tag wiki',"
        "    'Moderator nomination',"
        "    'Wiki placeholder',"
        "    'Privilege wiki',"
        "    'Linked'"
        "  ) NOT NULL,"
        "  AcceptedAnswerId BIGINT,"
        "  ParentId BIGINT,"
        "  CreationDate DATETIME NOT NULL,"
        "  DeletionDate DATETIME,"
        "  Score BIGINT NOT NULL,"
        "  ViewCount BIGINT,"
        "  Body MEDIUMTEXT NOT NULL,"
        "  OwnerUserId BIGINT,"
        "  OwnerDisplayName VARCHAR(255),"
        "  LastEditorUserId BIGINT,"
        "  LastEditorDisplayName VARCHAR(255),"
        "  LastEditDate DATETIME,"
        "  LastActivityDate DATETIME NOT NULL,"
        "  Title VARCHAR(511),"
        "  Tags VARCHAR(511),"
        "  AnswerCount BIGINT,"
        "  CommentCount BIGINT,"
        "  FavoriteCount BIGINT,"
        "  ClosedDate DATETIME,"
        "  CommunityOwnedDate DATETIME,"
        "  ContentLicense ENUM ("+contentLicenseVals+"),"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, ParentId)"
        ") " + tableOptions
    )
    cnx.cursor().execute(updateSiteEnums % "Posts")

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Tags ("
        "  SiteId ENUM ("+siteEnumVals+") NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  TagName VARCHAR(63),"
        "  Count BIGINT NOT NULL,"
        "  ExcerptPostId BIGINT,"
        "  WikiPostId BIGINT,"
        "  IsModeratorOnly BOOLEAN NOT NULL DEFAULT FALSE,"
        "  IsRequired BOOLEAN NOT NULL DEFAULT FALSE,"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, TagName)"
        ") " + tableOptions
    )
    cnx.cursor().execute(updateSiteEnums % "Tags")

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Users ("
        "  SiteId ENUM ("+siteEnumVals+") NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  Reputation BIGINT NOT NULL,"
        "  CreationDate DATETIME NOT NULL,"
        "  DisplayName VARCHAR(255),"
        "  LastAccessDate DATETIME,"
        "  WebsiteUrl VARCHAR(511),"
        "  Location VARCHAR(511),"
        "  AboutMe MEDIUMTEXT,"
        "  Views BIGINT NOT NULL,"
        "  UpVotes BIGINT NOT NULL,"
        "  DownVotes BIGINT NOT NULL,"
        "  ProfileImageUrl VARCHAR(511),"
        "  EmailHash VARCHAR(511),"
        "  AccountId BIGINT,"
        "  PRIMARY KEY(SiteId, Id)"
        ") " + tableOptions
    )
    cnx.cursor().execute(updateSiteEnums % "Users")

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Votes ("
        "  SiteId ENUM ("+siteEnumVals+") NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  PostId BIGINT NOT NULL,"
        "  VoteTypeId ENUM("
        "    'AcceptedByOriginator',"
        "    'UpMod',"
        "    'DownMod',"
        "    'Offensive',"
        "    'Favorite',"
        "    'Close',"
        "    'Reopen',"
        "    'BountyStart',"
        "    'BountyClose',"
        "    'Deletion',"
        "    'Undeletion',"
        "    'Spam',"
        "    '13',"
        "    '14',"    
        "    'ModeratorReview',"
        "    'ApproveEditSuggestion'"
        "  ) NOT NULL,"
        "  UserId BIGINT,"
        "  CreationDate DATETIME NOT NULL,"
        "  BountyAmount BIGINT,"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, PostId)"
        ") " + tableOptions
    )
    cnx.cursor().execute(updateSiteEnums % "Votes")

def checkColumnNames(data):
    for name in data.keys():
        assert re.match(r'^[a-zA-Z0-9_]+$',name), "Bad column name: " + name 

def insert(table, data):
    checkColumnNames(data)
    assert table, "No table specified"
    placeholders = ', '.join(['%s'] * len(data))
    columns = '`,`'.join(data.keys())
    sql = "INSERT INTO `%s` (`%s`) VALUES (%s);" % (table, columns, placeholders)
    cnx.cursor().execute(sql, list(data.values()))

def upsert(table, data):
    checkColumnNames(data)
    assert table, "No table specified"
    placeholders = ', '.join(['%s'] * len(data))
    columns = '`,`'.join(data.keys())
    updates = '`' + '`=%s,`'.join(data.keys()) + '`=%s'
    sql = "INSERT INTO `%s` (`%s`) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, updates)
    cnx.cursor().execute(sql, list(data.values()) + list(data.values()))

def querySite(url):
    return querySingle("SELECT * FROM Sites WHERE Url=%s",[url])

def querySingle(sql, args=[]):
    try:
        cursor = cnx.cursor()
        cursor.execute(sql, args)
        for row in cursor:
            return dict((column[0],row[index]) for index, column in enumerate(cursor.description))
        return {}
    finally:
        cursor.close()

def queryAll(sql, args=[]):
    try:
        cursor = cnx.cursor(dictionary=True, buffered=True)
        cursor.execute(sql, args)
        return cursor
    finally:
        cursor.close()

def querySites():
    sites=[]
    count = 1
    for row in queryAll("SELECT Id, TinyName FROM Sites ORDER BY Id"):
        id = row['Id']
        while (count < id):
            sites.append(str(count))
            count = count + 1
        sites.append(row['TinyName'])
        count = count + 1
    return sites

