import psycopg2
import pandas as pd
import bsavant_scraper as bs




table = bs.savant_search(2021, 'ARI', "Everything") #Try to find a more elegant solution


teams =[#'ARI',
        'ATL','BAL','BOS','CHC','CWS','CIN','CLE','COL','DET','HOU','KC','LAA','LAD','MIA',
        'MIL','MIN','NYM','NYY','OAK','PHI','PIT','SD','SEA','SF','STL','TB','TEX','TOR','WSH']


for team in teams:
    pd.concat([table,bs.savant_search(2021, team, "Everything")])
    print(team,'has',len(table.index),'pitches.')





#Conect to the database
conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="pass")


cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS standings')
cur.execute('CREATE TABLE standings (team INTEGER PRIMARY KEY, win INTEGER, loss INTEGER, lastTenWin INTEGER)')


for teams in standings_data:
    team = int(teams["teamId"])
    win = int(teams["win"])
    loss = int(teams["loss"])
    last10 = int(teams["lastTenWin"])
    cur.execute('''INSERT INTO standings (team, win, loss, lastTenWin)
                   VALUES (%s,%s,%s,%s)''',(team,win,loss,last10) )
    conn.commit()


    

#Close cursor
cur.close()

#Close connection
conn.close()


#Comparar vx0 y release_pos_x cual es la diferencia?


#Pitch exit velocity 
#spin 
#vertical break 
#Horizontal break 

#If the pitch was contacted 
#Exist velocity 
#Launch angle 