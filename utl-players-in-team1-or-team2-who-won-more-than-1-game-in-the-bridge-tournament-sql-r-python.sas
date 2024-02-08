%let pgm=utl-players-in-team1-or-team2-who-won-more-than-1-game-in-the-bridge-tournament-sql-r-python;

Players in team1 or team2 who won more than 1 game in the bridge tournament sql r python;

github
http://tinyurl.com/nfdrfrdf
https://github.com/rogerjdeangelis/utl-players-in-team1-or-team2-who-won-more-than-1-game-in-the-bridge-tournament-sql-r-python

 SOLUTION

    1. r sql
    2. python sql

Note python package pyreadr allows python to read an r dataframe

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                      |                              |                                                                  */
/*       INPUT          |        PROCESS               |        OUTPUT                                                    */
/*                      |                              |                                                                  */
/*  TEAM1      TEAM2    |    select                    |    TEAMS    PLAYER   WINS                                        */
/*                      |       "team_1"   as teams    |                                                                  */
/*  Alfred  *  Joyce    |       ,team1     as player   |   TEAM_1    Alfred      2                                        */
/*  Alfred  *  Judy     |       ,count(*)  as wins     |   TEAM_2     Plilip     2                                        */
/*  Alice      Louise   |    from                      |   TEAM_2      Roger     3                                        */
/*  Barbara    Mary     |        have                  |                                                                  */
/*  Carol      Philip * |    group                     |                                                                  */
/*  Henry      Philip * |        by team1              |                                                                  */
/*  James      Ronald   |    having                    |                                                                  */
/*  Jmes       Thomas   |        count(*) > 1          |                                                                  */
/*  Janet      William  |    union all                 |                                                                  */
/*  Jeffrey    Roger  * |    select                    |                                                                  */
/*  John       Roger  * |       "team_2"   as teams    |                                                                  */
/*  Jane       Roger  * |       ,team2     as player   |                                                                  */
/*                      |       ,count(*)  as wins     |                                                                  */
/*                      |    from                      |                                                                  */
/*                      |        have                  |                                                                  */
/*                      |    group                     |                                                                  */
/*                      |        by team2              |                                                                  */
/*                      |    having                    |                                                                  */
/*                      |        count(*) > 2          |                                                                  */
/*                      |                              |                                                                  */
/**************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

%utl_rbegin;
parmcards4;
unlink("d:/rds/have.rds");
have=read.table(header=TRUE,text="
TEAM1   TEAM2
Alfred  Joyce
Alfred  Judy
Alice   Louise
Barbara Mary
Carol   Philip
Henry   Philip
James   Ronald
Jmes    Thomas
Janet   William
Jeffrey Roger
John    Roger
Jane    Roger
");
saveRDS(have,file="d:/rds/have.rds");
have;
;;;;
%utl_rend;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  INPUT                                                                                                                 */
/*                                                                                                                        */
/*       TEAM1   TEAM2                                                                                                    */
/*                                                                                                                        */
/*  1   Alfred   Joyce                                                                                                    */
/*  2   Alfred    Judy                                                                                                    */
/*  3    Alice  Louise                                                                                                    */
/*  4  Barbara    Mary                                                                                                    */
/*  5    Carol  Philip                                                                                                    */
/*  6    Henry  Philip                                                                                                    */
/*  7    James  Ronald                                                                                                    */
/*  8     Jmes  Thomas                                                                                                    */
/*  9    Janet William                                                                                                    */
/*  10 Jeffrey   Roger                                                                                                    */
/*  11    John   Roger                                                                                                    */
/*  12    Jane   Roger                                                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                    _
/ |  _ __   ___  __ _| |
| | | `__| / __|/ _` | |
| | | |    \__ \ (_| | |
|_| |_|    |___/\__, |_|
                   |_|
*/

%utl_rbegin;
parmcards4;
library(sqldf);
have<-readRDS(file="d:/rds/have.rds");
have;
want<-sqldf('
  select
     "team_1"   as teams
     ,team1     as player
     ,count(*)  as wins
  from
      have
  group
      by team1
  having
      count(*) > 1
  union all
  select
     "team_2"   as teams
     ,team2     as player
     ,count(*)  as wins
  from
      have
  group
      by team2
  having
      count(*) > 1
');
want;
;;;;
%utl_rend;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*    TEAMS    PLAYER   WINS                                                                                              */
/*                                                                                                                        */
/*   TEAM_1    Alfred      2                                                                                              */
/*   TEAM_2    Plilip      2                                                                                              */
/*   TEAM_2     Roger      3                                                                                              */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                _   _                             _
|___ \   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
  __) | | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 / __/  | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|_____| | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/

%utl_pybegin;
parmcards4;
import pyreadr
from os import path
import pandas as pd
import numpy as np
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have = pyreadr.read_r('d:/rds/have.rds')[None]
want=pdsql('''
  select
     "team_1"   as teams
     ,team1     as player
     ,count(*)  as wins
  from
      have
  group
      by team1
  having
      count(*) > 1
  union all
  select
     "team_2"   as teams
     ,team2     as player
     ,count(*)  as wins
  from
      have
  group
      by team2
  having
      count(*) > 1
''');
print(want);
;;;;
%utl_pyend;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*      TEAMS  PLAYER  WINS                                                                                               */
/*                                                                                                                        */
/*  0  team_1  Alfred     2                                                                                               */
/*  1  team_2  Philip     2                                                                                               */
/*  2  team_2   Roger     3                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
