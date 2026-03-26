# EMPIRE_H2Imports


## Cloning the repository
If using git, execute ```git-lfs clone``` instead of ```git clone``` (otherwise the input data files are not downloaded correctly).


## Sending the repository to the server

Create a zip file containing the project. From a command prompt, execute the following command to send unzipped file to your folder on the server.
```
pscp [local path of the zip file] [user_id]@solstorm-login.iot.ntnu.no:[path of your folder in Solstorm]
```

## Package installation
From Putty or MobaXterm, execute:

```
module load Python/3.11.5-GCCcore-13.2.0
module load gurobi/9.5
pip install pandas==2.2.2
pip install cloudpickle==3.0.0
pip install openpyxl==3.1.5
pip install pyomo==6.7.3
pip install pyyaml==6.0.2
pip install scipy==1.14.0
pip install scikit-learn==1.5.1
pip install matplotlib==3.9.2
```

## Run
Connect to the solstorm serveur and go into an available node with the following command (from Putty or MobaXterm):
```
screen
ssh compute-[number_of_node]

```

When you are on the EMPIRE_CVAR folder, execute:

```
module load Python/3.11.5-GCCcore-13.2.0
module load gurobi/9.5
python run_EMPIRE.py
```

## Scenario Generation Method
Parameters value for random scenario generation (other scenario parameters set to default values):
```
scenariogeneration = True
FIX_SAMPLE = False
filter_make = False
filter_use = False
```

Parameters value for stratified scenario generation (other scenario parameters set to default values):
```
scenariogeneration = True
FIX_SAMPLE = False
filter_make = True
filter_use = True
n_cluster: int
```
Parameter ```filter_make``` can be set to False for the stratified method if a filter has already been defined and saved in csv file.


