**This project is deployed with docker containers.**

You must need a **github account with ssh key set up**

After creating github account you can follow below steps to set up ssh key

    ssh-keygen

Press enter to accept the default key and path

Enter and re-enter a passphrase when prompted.

    cat .ssh/id_rsa.pub (in case not default change the path after cat)

Copy the ssh key

**login to github** --> **click your avatar** --> **settings** --> under **Account settings** select **SSH and GPG keys**
click on **New SSH key** button **paste the copied key** inside **key** textarea click on **Add SSH key**

Take clone from bitbucket using below command

    cd ~
    git clone git@github.com:hsarbas/DRIVER2.0.git

Run production.sh from driver_new_tech

    cd driver_new_tech
    sudo bash production_host.sh

**Environment**

You must define the constant values in the .env file. A template .env.sample is provided which contains the keys and
values required to run DRIVER2.0.

Create .env file in the project directory refer template .env.sample follow steps below:

    cd /var/www/driver_new_tech/
    sudo nano .env
    save and close the file

In the project directory(/var/www/driver_new_tech/) as a superuser, execute steps below:

    docker-compose up -d

There are 5 containers running as daemons in order to make DRIVER2.0 available. The nginx server is set up at the host
in the standard mode.

Review the containers using below command

    docker ps

Execute the configure.sh file using below commands.

    cd /var/www/driver_new_tech/
    ./configure.sh

Enter username,email,password for superuser.

Execute the script to create initial dataset by using below command. "driver-new-tech" is a name of docker 
container you can replace it by yours(in case modified).

    docker exec "driver-new-tech" ./manage.py create_dataset

Create Incident and Intervention schema using below commands.

    docker exec "driver-new-tech" python ./scripts/load_incident_schema.py --api-url 'http://{{ip_addr/domain_name}}/api'
    
    docker exec "driver-new-tech" python ./scripts/load_intervention_schema.py  --api-url 'http://{{ip_addr/domain_name}}/api'

    docker exec "driver-new-tech" python ./scripts/load_black_spots.py  --api-url 'http://{{ip_addr/domain_name}}/api'

Add English language for both Admin&User panel using below command.

    docker exec "driver-new-tech" python ./scripts/load_default_languages.py --api-url 'http://{{ip_addr/domain_name}}/api'

Change API HOST for:

Admin Panel

    sudo nano Ashlar-Editor/dist/web-driver-admin/index.html
    change <app-root hostname = "http://{{ip_addr/domain_name}}/"> to running ip ip_addr/domain_name

User Panel

    sudo nano User-Panel/dist/WB-Driver/index.html
    change <app-root hostname = "http://{{ip_addr/domain_name}}/" windshaftUrl = "http://{{ip_addr/domain_name}}"></app-root> to running ip_addr/domain_name

Link for accessing User & Admin panel:

User panel "http://{{ip_addr/domain_name}}/"

Admin panel "http://{{ip_addr/domain_name}}/editor/"

**To login into the User Panel, it is mandatory to add the Geography files (City/Province or Regions).**

Once this is completed, the following needs to be installed from the Admin Panel:
The SuperAdmin now needs to login to the Ashlar Editor using the credentials built while creating SuperAdmin from the
database. (eg. username:- driver, password:- driver).

Once the user is logged in into the Ashlar Editor, they first need to add the shapefiles for the city and region. To do
so, the user should follow the following steps:

All Geography -> Add New Geography -> (for City/Province)
Geography Label -> City/Province Display Field (select after upload) -> name (this will only be enabled when the user
saves the label, file, and Geography Color)
Geography Color -> red All Geography -> Add New Geography -> (for Regions)
Geography Label -> Regions Display Field (select after upload) -> region (this will only be enabled when the user saves
the label, file, and Geography Color)
Geography Color -> red

After uploading the shapefiles, the user now needs to set permissions for the already existing 2 user groups i.e.
SuperAdmin and Public. To set the permissions for these 2 groups, the user needs to follow the following steps:
Manage Permissions -> Click on “Edit” in front of the SuperAdmin -> The user needs to check the checkbox in front of “Is
admin” and check the checkboxes against the following: record, recordduplicate, recordauditlogentry, savedfilter. For
Public, the user needs to click on the Edit button against the Public group name and check the checkboxes against it:
sendrolerequest, savedfilter. Now the Administrator needs to create 4 more user groups and add permissions to it. To do
so, the Administrator needs to click on “Add Group Permission”. On the Assign Permission the Administrator needs to add
the “Group Name”, “Description”, check on Is Admin (only for the Admin Users i.e. City/Organizational Admin and Regional
Admin), and add the permissions. The permissions will be as follows:
Regional Admin -> record, recordduplicate, sendrolerequest, savedfilter. City/Organizational Admin -> record,
recordduplicate, sendrolerequest, savedfilter. Analyst -> record, sendrolerequest, savedfilter. Tech Analyst -> In the
record, the Administrator needs to click on “Can view record ”, sendrolerequest, savedfilter Public -> In the record,
the Administrator needs to click on “Can view record ”, sendrolerequest, savedfilter.

NOTE:- We will be writing an automated script for all these permissions so that they can be set automatically.

Once all the permissions are set, the user needs to “Add New Record Type”. The 2 record types are Incident and
Intervention. This can be done, by All Record Types -> Add a new record type. Single Title -> Incident Plural Title ->
Incidents Description -> Historical incident data

Single Title -> Intervention Plural Title -> Interventions Description ->Actions to improve traffic safety

This will create 2 items in the side navbar. Now the user can create the required schema for the Incident and
Intervention form. The following should fall under Incidents:
Incident Details Severity -> Select list Display Type -> checkbox Option value -> fatal, Injury, Property Damage
Filterable/Searchable -> checked

Main cause -> Select list Display Type -> select Option value -> Vehicle defect, Road defect, Human error, Other (see
description)
Filterable/Searchable -> checked

Collision type -> Select List Display Type -> select Option value -> Head on, Rear end, Right angle, Angle (Other), Side
swipe, Overturned vehicle, Hit object in road, Hit object off road, Hit parked vehicle, Hit pedestrian, Hit animal,
Other (see description)
Filterable/Searchable -> checked

Reporting Agency -> Select list Display Type -> select Option value -> PNP, Local Traffic Unit, BGCEA, CCTO, MMDA
Metrobase, Dipolog City TMO, Davao City TMO, MMDA Road Safety Unit, Muntinlupa Traffic Management Bureau, Pasig TPMO,
Zamboanga Admin Office, LDRRMO - Liloy, Other Filterable/Searchable -> checked

Location Approximate -> Select List Display Type -> checkbox Option value -. Yes

Email of Encoder -> Text Field Text Options -> Single Line Text Filterable/Searchable -. Checked

Crash Diagrams:
Go to -> Incident -> Add Related Content ->
Single Title -> Crash Diagram Plural Title -> Crash Diagrams Description -> Crash Diagram

Crash Type -> Select list Option Value -> Pedestrian, Maneuvering, etc. Movement Code -> Text Field Text Options ->
Single line Text Image -> Image Uploader

Vehicles:
Incident -> Add Related Content ->
Single Title -> Vehicle Plural Title -> Vehicles Description -> A vehicle involved in the incident Allow multiple? ->
checked

Classification -> Select List Display Type -> select Option value -> Private, Government, Public / For-Hire, Diplomat

Vehicle type -> Select List Display Type -> select Option value -> Car, Van, SUV, Bus, Jeepney, Taxi (metered), Truck (
Pick-Up), Truck (Rigid), Truck (Articulated), Truck (Fire), Truck (Unknown), Ambulance, Armored Car, Heavy Equipment,
Motorcycle, Tricycle, Bicycle, Pedicab, Pedestrian, Push-Cart, Horse-Driven Carriage (Tartanilla), Animal, Water Vessel,
Electric Bike, Others, Habal-habal Filterable/Searchable -> checked

Make -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

Plate number -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

Model -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

Maneuver -> Select List Display Type -> select Option value -> Left turn, Right turn, "U" turn, Cross traffic, Merging,
Diverging, Overtaking, Going ahead, Reversing, Sudden start, Sudden stop, Parked off road, Parked on road

Damage -> Select List Display Type -> select Option value -> None, Front, Rear, Right, Left, Roof, Multiple

Defect -> Select List Display Type -> select Option value -> None, Lights, Brakes, Steering, Tires, Multiple

Loading -> Select List Display Type -> select Option value -> Legal, Overloaded, Unsafe Load, Other (see description)

Insurance details -> Text Field Text Options -> Paragraph Text Filterable/Searchable -> checked

Engine number -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

Chassis number -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

People Single Title -> Person Plural Title -> People Description -> A person involved in the incident Allow multiple? ->
checked

Involvement -> Select List Display Type -> select Option value -> Pedestrian, Witness, Passenger, Driver

First Name -> Text Field Text Options -> Single Line text Filterable/Searchable -> checked

Middle Name -> Text Field Text Options -> Single Line text Filterable/Searchable -> checked

Last Name -> Text Field Text Options -> Single Line text Filterable/Searchable -> checked

Address -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

Gender -> Select List Display Type -> select Option value -> Male, Female, Other

License Number -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

Age -> Text Field Text Options -> Number

Driver error -> Select List Display Type -> select Option value -> Fatigued/asleep, Inattentive, Too fast, Too close, No
signal, Bad overtaking, Bad turning, Using cell phone

Injury -> Select List Display Type -> select Option value -> Fatal, Serious, Minor, Not Injured

Alcohol/drugs -> Select List Display Type -> select Option value -> Alcohol suspected, Drugs suspected

Seat belt/helmet -> Select List Display Type ->  select Option value -> Seat belt/helmet worn, Not worn, Not worn
correctly

Hospital -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

Vehicle -> Relationship Type of related info to reference -> Vehicle

Photos Single Title -> Photo Plural Title -> Photos Description -> Pictures of the incident Allow multiple? -> checked

Picture -> Image Uploader Required -> checked

Description -> Text Field Text Options -> Single Line Text Filterable/Searchable -> checked

Notes Single Title -> Notes Plural Title -> Notes Description -> Notes

Notes -> Text Field Text Options -> Paragraph Text

For Intervention, the schema can be prepared as follows:

Type -> Select List Option value -> Addressing Alcohol and Other Drugs, Additional Lane, Bicycle Facilities, Central
Hatching, Central Turning Lane Full Length, Child Safety Initiatives, Delineation, Duplication, Emergency Response,
Enforcement, Fatigue Management, Intersection - Delineation, Intersection - Grade Separation, Intersection - Roundabout,
Intersection - Signalise, Intersection - Turn Lanes (Signalised), Intersection - Turn Lanes (Unsignalised), Lane
Widening, Median barrier, Median Crossing Upgrade, Motorcycle Lanes, One Way Network, Parking Improvements, Pedestrian
Crossing - Grade Separation, Pedestrian Crossing - Signalised, Pedestrian Crossing - Unsignalised, Pedestrian Fencing,
Pedestrian Footpath, Pedestrian Refuge Island, Publicity, Railway Crossing, Realignment - Horizontal, Realignment -
Vertical, Regulate Roadside Commercial Activity, Restrict/Combine Direct Access Points, Road Surface Rehabilitation,
Roadside Safety - Barriers, Roadside Safety - Hazard Removal, Rumble Strips, Safe Speed, School Zones, Seatbelts,
Service Road, Shoulder Sealing, Sideslope Improvement, Sight Distance (obstruction removal), Skid Resistance, Speed
Management, Street Lighting, Traffic Calming, Used Car Safety Ratings, Vehicle Features and Devices, Vehicle
Roadworthiness, Education, Helmets and Protective Clothing, Licensing, Motor Vehicle Standards, New Car Assessment
Program (NCAP)
Required -> checked
