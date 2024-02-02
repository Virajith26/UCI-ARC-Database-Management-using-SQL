import mysql.connector
from constants import Constants

class Queries(object):
    """Database queries"""
    connection = mysql.connector.connect(user=Constants.USER, password=Constants.PASSWORD, database=Constants.DATABASE)
    cursor = connection.cursor()
    
    # 1. Retrieve the names and genders of all people associated with ARC (i.e., members, employees, etc.)
    query1 = "select name, gender from person;"
    
    #2. List the names and departments of all faculty members who are also members of ARC. 
    query2 = "select person.name, department from person inner join (select university_affiliate.card_id,department from university_affiliate inner join(select card_id from non_student where member_type='Faculty') as faculty_members on university_affiliate.card_id=faculty_members.card_id) as faculty_department on person.card_id=faculty_department.card_id;"
    
    # 3. Find the names of the people who were present in either the weight room or the cardio room on 2023-04-01.
    query3 = "select distinct(name) from person inner join (select timestamp_person.person_id,timestamp_person.space_id from (select person_id,space_id from location_reading where timestamp='2023-04-01 00:00:00') as timestamp_person inner join (select space_id,description from space where description='weight room' or description='cardio room') as weight_cardio on timestamp_person.space_id=weight_cardio.space_id) as per_sp on per_sp.person_id=person.card_id;"
    
    # 4. Find the names of the people who have attended all events.
    query4 = "select person.name from person natural join attends natural join events Group by person.name having count(events.description) >=(select count(distinct description) from events);"

    # 5. List the events whose capacity have reached the maximum capacity of their associated space. (Just project the event ids)
    query5 = "select event_id from events inner join space where events.space_id=space.space_id and events.capacity>=space.max_capacity;"

    #6. Find the names of students who have used all the equipment located in the cardio room.
    query6 = "select person.name from student natural join person natural join usage_reading natural join space natural join equipment where space.description = 'cardio room' group by person.name having count(equipment.equipment_type) >= (select count(distinct equipment.equipment_type) from space natural join equipment where space.description = 'cardio room');"

    #7. List the equipment ids and types for equipment that is currently in use.
    query7 = "select equipment_id,equipment_type from equipment where is_available=1;"
    
    #8. Find names of all employees in ARC.
    query8 = "select name from person inner join employee on person.card_id=employee.card_id;"
    
    #9. Retrieve the names of all members who have attended an event in the yoga studio.
    query9 = "select distinct(name) from (select card_id from (select event_id from (select space_id from space where description='yoga studio') as yoga inner join events on yoga.space_id=events.space_id) as yoga_events inner join attends on attends.event_id=yoga_events.event_id) as yoga_events_ha inner join person on person.card_id=yoga_events_ha.card_id;"
    
    #10. Find all family members who have attended ‘Summer Splash Fest’
    query10 = "select person.name from events natural join attends natural join person natural join family where events.description = 'Summer Splash Fest';"

    #11.Calculate the average hourly rate paid to all employees who are of student type at ARC 
    query11 = " select avg(salary_hour) from employee where employee_type='student';"

    #12. Find the name of the Trainer(s) with the 2nd highest average hourly rate
    query12 = "select ranks.name from (select trainers.name,employee.salary_hour,DENSE_RANK() over(order by salary_hour desc) rownumber from (select person.name,card_id from person inner join Trainer on person.card_id=Trainer.person_id) as trainers inner join employee on employee.card_id=trainers.card_id) as ranks where ranks.rownumber=2;"

    #13. Calculate the total number of days Mekhi Sporer visited the weight room.
    query13 = "select timestamps.days from (select count(timestamp) as days,person_id from location_reading inner join space on space.space_id=location_reading.space_id group by person_id) as timestamps inner join person on person.card_id=timestamps.person_id where name='Mekhi Sporer';"

    #14. Find the names of member(s) who spent the most time in the cardio room in the month of May
    query14 = "select person.name from (select member.card_id from (select person_id,dense_rank() over(order by COUNT(DISTINCT DATE(timestamp)) desc) rownumber from location_reading inner join space on space.space_id=location_reading.space_id where location_reading.timestamp like '%-05-%' and space.description='cardio room' group by person_id ) as cardios inner join member on cardios.person_id=member.card_id where cardios.rownumber=1) as peeps inner join person on person.card_id=peeps.card_id;"

    #15. Find the name and the average occupancy of the space which has the lowest average occupancy per event. ( you need to get the occupancy from the attends table)
    query15 = "select space.description, CAST(anss.peoples AS DECIMAL)/CAST(anss.eventscount AS DECIMAL) as avg from (select events.space_id, count(eventss.event_id) as eventscount,sum(eventss.people) as peoples from (select event_id,count(card_id) as people  from attends group by event_id) as eventss inner join events on events.event_id=eventss.event_id group by space_id) as anss inner join space on space.space_id=anss.space_id order by avg limit 1;"
    
    cursor.execute(query1)
    
    print(cursor.fetchall())