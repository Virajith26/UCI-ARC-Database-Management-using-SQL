from relational_algebra import *
import sqlite3
class Expressions():

    # 1) Retrieve the name of all Trainers who have the credentials CNS
    sample_query = Projection(NaturalJoin(Selection(Relation("Trainer"),Equals("credentials","CNS")),Relation("person")),["name"])
    
    # 1) Retrieve the names and genders of all people associated with ARC (i.e., members, employees, etc.)
    expression1 = Projection(Relation("Person"),["name","gender"])

    # 2) List the names and departments of all faculty members who are also members of ARC
    expression2 = Projection(NaturalJoin(NaturalJoin(Selection(Relation("non_student"),Equals("member_type","Faculty")),Relation("university_affiliate")),Relation("person")),["name","department"])
    
    # 3) Find all the people who were present in either the weight room or the cardio room on 2023-04-01
    expression3 =  Projection(ThetaJoin(NaturalJoin(Selection(Relation("space"),Or(Equals("space_description","weight room"),Equals("space_description","cardio room"))),Selection(Relation("location_reading"),Equals("timestamp","2023-04-01 00:00:00"))),Relation("person"),Equals("person_id","card_id")),["name"])
    
    # 4) Find the names of people who have attended all events
    expression4 =  Projection(NaturalJoin(Projection(NaturalJoin(Division(Projection(Relation("attends"),["card_id","event_id"]),Projection(Relation("events"),["event_id"])),Relation("attends")),["card_id"]),Relation("person")),["name"])
    
    # 5) List the events whose capacity have reached the reached maximum capacity of their associated space.
    expression5 = Projection(Selection(NaturalJoin(Relation("events"),Relation("space")),GreaterEquals("capacity","max_capacity")),["event_id"])

    # 6) Find the students who have used all the equipment located in the cardio room
    expression6 = Projection(NaturalJoin(Division(Projection(NaturalJoin(NaturalJoin(Projection(NaturalJoin(Selection(Relation("space"),Equals("space_description","cardio room")),Relation("equipment")),["equipment_id"]),Relation("usage_reading")),Relation("student")),["card_id","equipment_id"]),Projection(NaturalJoin(Selection(Relation("space"),Equals("space_description","cardio room")),Relation("equipment")),["equipment_id"])),Relation("person")),["name"])

    #expression7 =

    #expression8 =

    #expression9 =

    #expression10 =


    # sql_con = sqlite3.connect("sample220P.db")

    # result = expression6.evaluate(sql_con=sql_con)
    # print(result.rows)