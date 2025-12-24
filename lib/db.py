from mysql.connector import connect
from lib import localConfig
import re

dbConf = localConfig.getDict('mysql')
cnx = connect(
    user=dbConf['user'],
    password=dbConf['password'],
    host=dbConf['host'],
    database=dbConf['database'],
    port=dbConf['port'],
    auth_plugin=dbConf['auth_plugin'],
    charset='utf8mb4',
    use_unicode=True,
    ssl_disabled=True
)

def createSchema():
    # Stack Exchange schema documentation
    # https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede

    tableOptions="ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMPRESSION='ZLIB';"

    contentLicenseVals="'CC BY-SA 2.5','CC BY-SA 3.0','CC BY-SA 4.0'"

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Sites ("
        "  Id BIGINT PRIMARY KEY NOT NULL,"
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
        "  LastPost DATETIME,"
        "  UNIQUE INDEX (TinyName),"
        "  UNIQUE INDEX (Name),"
        "  UNIQUE INDEX (LongName),"
        "  UNIQUE INDEX (Url)"
        ") " + tableOptions
    )

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Badges ("
        "  SiteId BIGINT NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  UserId BIGINT NOT NULL,"
        "  Name VARCHAR(255) NOT NULL,"
        "  Date DATETIME NOT NULL,"
        "  Class BIGINT NOT NULL,"
        "  TagBased BOOLEAN NOT NULL,"
        "  PRIMARY KEY (SiteId, Id),"
        "  INDEX (SiteId, UserId)"
        ") " + tableOptions
    )

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Comments ("
        "  SiteId BIGINT NOT NULL,"
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

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS PostHistoryTypes ("
        "  Id BIGINT PRIMARY KEY,"
        "  Name VARCHAR(255),"
        "  Stage ENUM('Initial','Edit','Rollback'),"
        "  Field ENUM('Title','Body','Tags')"
        ") " + tableOptions
    )
    for data in [
        [1,'Initial Title','Initial','Title'],
        [2,'Initial Body','Initial','Body'],
        [3,'Initial Tags','Initial','Tags'],
        [4,'Edit Title','Edit','Title'],
        [5,'Edit Body','Edit','Body'],
        [6,'Edit Tags','Edit','Tags'],
        [7,'Rollback Title','Rollback','Title'],
        [8,'Rollback Body','Rollback','Body'],
        [9,'Rollback Tags','Rollback','Tags'],
        [10,'Post Closed'],
        [11,'Post Reopened'],
        [12,'Post Deleted'],
        [13,'Post Undeleted'],
        [14,'Post Locked'],
        [15,'Post Unlocked'],
        [16,'Community Owned'],
        [17,'Post Migrated'],
        [18,'Question Merged'],
        [19,'Question Protected'],
        [20,'Question Unprotected'],
        [21,'Post Disassociated'],
        [22,'Question Unmerged'],
        [24,'Suggested Edit Applied'],
        [25,'Post Tweeted'],
        [31,'Comment discussion moved to chat'],
        [33,'Post notice added '],
        [34,'Post notice removed'],
        [35,'Post migrated away'],
        [36,'Post migrated here'],
        [37,'Post merge source'],
        [38,'Post merge destination'],
        [50,'Bumped by Community User'],
        [52,'Question became hot'],
        [53,'Question removed from hot']
    ]:
        upsert("PostHistoryTypes", {"Id":data[0], "Name":data[1], "Stage":(data[2] if len(data)>2 else None), "Field":(data[3] if len(data)>3 else None)})
    cnx.commit()

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS PostHistory ("
        "  SiteId BIGINT NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  PostHistoryTypeId BIGINT NOT NULL,"
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

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS PostLinkTypes ("
        "  Id BIGINT PRIMARY KEY,"
        "  Name VARCHAR(255)"
        ") " + tableOptions
    )
    for data in [
        [1,'Linked'],
        [3,'Duplicate']
    ]:
        upsert("PostLinkTypes", {"Id":data[0], "Name":data[1]})
    cnx.commit()

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS PostLinks ("
        "  SiteId BIGINT NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  CreationDate DATETIME NOT NULL,"
        "  PostId BIGINT NOT NULL,"
        "  RelatedPostId BIGINT NOT NULL,"
        "  LinkTypeId BIGINT NOT NULL,"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, PostId)"
        ") " + tableOptions
    )

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS PostTypes ("
        "  Id BIGINT PRIMARY KEY,"
        "  Name VARCHAR(255)"
        ") " + tableOptions
    )
    for data in [
        [1,'Question'],
        [2,'Answer'],
        [3,'Orphaned tag wiki'],
        [4,'Tag wiki excerpt'],
        [5,'Tag wiki'],
        [6,'Moderator nomination'],
        [7,'Wiki placeholder'],
        [8,'Privilege wiki'],
        [9,'Linked']
    ]:
        upsert("PostTypes", {"Id":data[0], "Name":data[1]})
    cnx.commit()

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Posts ("
        "  SiteId BIGINT NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  PostTypeId BIGINT NOT NULL,"
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

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Tags ("
        "  SiteId BIGINT NOT NULL,"
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

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Users ("
        "  SiteId BIGINT NOT NULL,"
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

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS VoteTypes ("
        "  Id BIGINT PRIMARY KEY,"
        "  Name VARCHAR(255)"
        ") " + tableOptions
    )
    for data in [
        [1,'AcceptedByOriginator'],
        [2,'UpMod'],
        [3,'DownMod'],
        [4,'Offensive'],
        [5,'Favorite'],
        [6,'Close'],
        [7,'Reopen'],
        [8,'BountyStart'],
        [9,'BountyClose'],
        [10,'Deletion'],
        [11,'Undeletion'],
        [12,'Spam'],
        [15,'ModeratorReview'],
        [16,'ApproveEditSuggestion']
    ]:
        upsert("VoteTypes", {"Id":data[0], "Name":data[1]})
    cnx.commit()

    cnx.cursor().execute(
        "CREATE TABLE IF NOT EXISTS Votes ("
        "  SiteId BIGINT NOT NULL,"
        "  Id BIGINT NOT NULL,"
        "  PostId BIGINT NOT NULL,"
        "  VoteTypeId BIGINT NOT NULL,"
        "  UserId BIGINT,"
        "  CreationDate DATETIME NOT NULL,"
        "  BountyAmount BIGINT,"
        "  PRIMARY KEY(SiteId, Id),"
        "  INDEX (SiteId, PostId)"
        ") " + tableOptions
    )
    
    cnx.cursor().execute(
        "CREATE OR REPLACE VIEW FullSites AS"
        "  SELECT"
        "    site.*,"
        "    parent.TinyName AS ParentTinyName,"
        "    parent.Name AS ParentName,"
        "    parent.LongName AS ParentLongName,"
        "    parent.Url AS ParentUrl,"
        "    parent.ImageUrl AS ParentImageUrl,"
        "    parent.IconUrl AS ParentIconUrl,"
        "    parent.DatabaseName AS ParentDatabaseName,"
        "    parent.Tagline AS ParentTagline,"
        "    parent.TagCss AS ParentTagCss,"
        "    parent.ODataEndpoint AS ParentODataEndpoint,"
        "    parent.BadgeIconUrl AS ParentBadgeIconUrl,"
        "    parent.ImageBackgroundColor AS ParentImageBackgroundColor,"
        "    parent.TotalQuestions AS ParentTotalQuestions,"
        "    parent.TotalAnswers AS ParentTotalAnswers,"
        "    parent.TotalUsers AS ParentTotalUsers,"
        "    parent.TotalComments AS ParentTotalComments,"
        "    parent.TotalTags AS ParentTotalTags,"
        "    parent.LastPost AS ParentLastPost"
        "  FROM"
        "    Sites site"
        "  LEFT JOIN"
        "    Sites parent"
        "  ON"
        "    site.ParentId = parent.Id"
    )

    cnx.cursor().execute(
        "CREATE OR REPLACE VIEW FullBadges AS"
        "  SELECT"
        "    badge.*,"
        "    site.TinyName AS SiteTinyName,"
        "    site.Name AS SiteName,"
        "    site.LongName AS SiteLongName,"
        "    site.Url AS SiteUrl,"
        "    user.Reputation AS UserReputation,"
        "    user.CreationDate AS UserCreationDate,"
        "    user.DisplayName AS UserDisplayName,"
        "    user.LastAccessDate AS UserLastAccessDate,"
        "    user.Views AS UserViews,"
        "    user.UpVotes AS UserUpVotes,"
        "    user.DownVotes AS UserDownVotes,"
        "    user.AccountId AS UserAccountId"
        "  FROM"
        "    Badges badge"
        "  LEFT JOIN"
        "    Sites site"
        "  ON"
        "    badge.SiteId = site.Id"
        "  LEFT JOIN"
        "    Users user"
        "  ON"
        "    badge.UserId = user.Id AND"
        "    badge.SiteId = user.SiteId"
    )

    cnx.cursor().execute(
        "CREATE OR REPLACE VIEW FullComments AS"
        "  SELECT"
        "    comment.SiteId,"
        "    comment.Id,"
        "    comment.PostId,"
        "    comment.Score,"
        "    comment.Text,"
        "    comment.CreationDate,"
        "    comment.UserId,"
        "    comment.ContentLicense,"
        "    site.TinyName AS SiteTinyName,"
        "    site.Name AS SiteName,"
        "    site.LongName AS SiteLongName,"
        "    site.Url AS SiteUrl,"
        "    user.Reputation AS UserReputation,"
        "    user.CreationDate AS UserCreationDate,"
        "    COALESCE(user.DisplayName, comment.UserDisplayName) AS UserDisplayName,"
        "    user.LastAccessDate AS UserLastAccessDate,"
        "    user.Views AS UserViews,"
        "    user.UpVotes AS UserUpVotes,"
        "    user.DownVotes AS UserDownVotes,"
        "    user.AccountId AS UserAccountId"
        "  FROM"
        "    Comments comment"
        "  LEFT JOIN"
        "    Sites site"
        "  ON"
        "    comment.SiteId = site.Id"
        "  LEFT JOIN"
        "    Users user"
        "  ON"
        "    comment.UserId = user.Id AND"
        "    comment.SiteId = user.SiteId"
    )

    cnx.cursor().execute(
        "CREATE OR REPLACE VIEW FullPostHistory AS"
        "  SELECT"
        "    postHistory.SiteId,"
        "    site.TinyName AS SiteTinyName,"
        "    site.Name AS SiteName,"
        "    site.LongName AS SiteLongName,"
        "    site.Url AS SiteUrl,"
        "    postHistory.Id,"
        "    postHistory.PostHistoryTypeId,"
        "    postHistoryType.Name AS PostHistoryTypeName,"
        "    postHistoryType.Stage AS PostHistoryTypeStage,"
        "    postHistoryType.Field AS PostHistoryTypeField,"
        "    postHistory.PostId,"
        "    postHistory.RevisionGUID,"
        "    postHistory.CreationDate,"
        "    postHistory.UserId,"
        "    user.Reputation AS UserReputation,"
        "    user.CreationDate AS UserCreationDate,"
        "    COALESCE(user.DisplayName, postHistory.UserDisplayName) AS UserDisplayName,"
        "    user.LastAccessDate AS UserLastAccessDate,"
        "    user.Views AS UserViews,"
        "    user.UpVotes AS UserUpVotes,"
        "    user.DownVotes AS UserDownVotes,"
        "    user.AccountId AS UserAccountId,"
        "    postHistory.Comment,"
        "    postHistory.Text,"
        "    postHistory.ContentLicense"
        "  FROM"
        "    PostHistory postHistory"
        "  LEFT JOIN"
        "    Sites site"
        "  ON"
        "    postHistory.SiteId = site.Id"
        "  LEFT JOIN"
        "    PostHistoryTypes postHistoryType"
        "  ON"
        "    postHistory.PostHistoryTypeId = postHistoryType.Id"
        "  LEFT JOIN"
        "    Users user"
        "  ON"
        "    postHistory.UserId = user.Id AND"
        "    postHistory.SiteId = user.SiteId"
    )

    cnx.cursor().execute(
        "CREATE OR REPLACE VIEW FullPostLinks AS"
        "  SELECT"
        "    postLink.*,"
        "    site.TinyName AS SiteTinyName,"
        "    site.Name AS SiteName,"
        "    site.LongName AS SiteLongName,"
        "    site.Url AS SiteUrl,"
        "    postLinkType.Name AS LinkTypeName"
        "  FROM"
        "    PostLinks postLink"
        "  LEFT JOIN"
        "    Sites site"
        "  ON"
        "    postLink.SiteId = site.Id"
        "  LEFT JOIN"
        "    PostLinkTypes postLinkType"
        "  ON"
        "    postLink.LinkTypeId = postLinkType.Id"
    )

    cnx.cursor().execute(
        "CREATE OR REPLACE VIEW FullPosts AS"
        "  SELECT"
        "    post.SiteId,"
        "    post.Id,"
        "    post.PostTypeId,"
        "    post.AcceptedAnswerId,"
        "    post.ParentId,"
        "    post.CreationDate,"
        "    post.DeletionDate,"
        "    post.Score,"
        "    post.ViewCount,"
        "    post.Body,"
        "    post.OwnerUserId,"
        "    post.LastEditorUserId,"
        "    post.LastEditDate,"
        "    post.LastActivityDate,"
        "    post.Title,"
        "    post.Tags,"
        "    post.AnswerCount,"
        "    post.CommentCount,"
        "    post.FavoriteCount,"
        "    post.ClosedDate,"
        "    post.CommunityOwnedDate,"
        "    post.ContentLicense,"
        "    site.TinyName AS SiteTinyName,"
        "    site.Name AS SiteName,"
        "    site.LongName AS SiteLongName,"
        "    site.Url AS SiteUrl,"
        "    owner.Reputation AS OwnerReputation,"
        "    owner.CreationDate AS OwnerCreationDate,"
        "    COALESCE(owner.DisplayName, post.OwnerDisplayName) AS OwnerDisplayName,"
        "    owner.LastAccessDate AS OwnerLastAccessDate,"
        "    owner.Views AS OwnerViews,"
        "    owner.UpVotes AS OwnerUpVotes,"
        "    owner.DownVotes AS OwnerDownVotes,"
        "    owner.AccountId AS OwnerAccountId,"
        "    lastEditor.Reputation AS LastEditorReputation,"
        "    lastEditor.CreationDate AS LastEditorCreationDate,"
        "    COALESCE(lastEditor.DisplayName, post.LastEditorDisplayName) AS LastEditorDisplayName,"
        "    lastEditor.LastAccessDate AS LastEditorLastAccessDate,"
        "    lastEditor.Views AS LastEditorViews,"
        "    lastEditor.UpVotes AS LastEditorUpVotes,"
        "    lastEditor.DownVotes AS LastEditorDownVotes,"
        "    lastEditor.AccountId AS LastEditorAccountId,"
        "    postType.Name AS PostTypeName"
        "  FROM"
        "    Posts post"
        "  LEFT JOIN"
        "    Sites site"
        "  ON"
        "   post.siteId = site.Id"
        "  LEFT JOIN"
        "    Users owner"
        "  ON"
        "    post.OwnerUserId = owner.Id AND"
        "    post.SiteId = owner.SiteId"
        "  LEFT JOIN"
        "    Users lastEditor"
        "  ON"
        "    post.OwnerUserId = lastEditor.Id AND"
        "    post.SiteId = lastEditor.SiteId"
        "  LEFT JOIN"
        "    PostTypes postType"
        "  ON"
        "    post.PostTypeId = postType.Id"
    )

    cnx.cursor().execute(
        "CREATE OR REPLACE VIEW FullTags AS"
        "  SELECT"
        "    tag.*,"
        "    site.TinyName AS SiteTinyName,"
        "    site.Name AS SiteName,"
        "    site.LongName AS SiteLongName,"
        "    site.Url AS SiteUrl"
        "  FROM"
        "    Tags tag"
        "  LEFT JOIN"
        "    Sites site"
        "  ON"
        "    tag.SiteId = site.Id"
    )

    cnx.cursor().execute(
        "CREATE OR REPLACE VIEW FullUsers AS"
        "  SELECT"
        "    user.*,"
        "    site.TinyName AS SiteTinyName,"
        "    site.Name AS SiteName,"
        "    site.LongName AS SiteLongName,"
        "    site.Url AS SiteUrl"
        "  FROM"
        "    Users user"
        "  LEFT JOIN"
        "    Sites site"
        "  ON"
        "    user.SiteId = site.Id"
    )

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

