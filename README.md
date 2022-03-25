
# NovHack 2022 Development Environment

This is the development environment that your group can use during the NovHack 2022.
Use this space to try out, explore, develop and finally submit your solution.

/!\ Fork this repository in your own github account and give other members of your group the rights to read/write in that repository.

## Setup
1. Check if you have the correct java version 

```
java --version
```
If java is not installed or if the version is below OpenJDK 11, 
download the latest version of Java 11. https://www.oracle.com/be/java/technologies/javase/jdk11-archive-downloads.html

2. To execute the code locally, download the IntelliJ IDE if you don't have it yet. 
Download the community edition avalable here https://www.jetbrains.com/fr-fr/idea/download/#section=linux
If you are on linux, you can also download it via the Ubuntu Software Store.


3. Launch the setup of your environement with:
```
./setup.sh
```
Enter the information given at the start of the hackathon.
This script will:
* Configure the AWS CLI for you
* Download your configuration file
* Download the historical data
* Download SQL initialization script
* Setup SQL client on your machine

4. Configure your IDE to run the code. The detailed instructions are listed
here: https://docs.google.com/document/d/1XDWkBU49dmF5JBVskEHGcslIvPFNTpkvZEgD2Rk2c48/edit?usp=sharing




## Run code locally 

TODO
wget https://archive.apache.org/dist/flink/flink-1.13.2/flink-1.13.2-bin-scala_2.12.tgz
tar -xzf flink-1.13.2-bin-scala_2.12.tgz

./bin/start-cluster.sh
./bin/flink run path/to/app.jar

## Submit code

You should have a bash script file at the root of the project called 
```submit.sh```

This script packages your application into a JAR file and sends it directly to AWS where we will get your files. Since the hackathon includes two main tasks, the submission script sends the 2 requested JAR files. 

:warning: The `submit.sh` expects that the 2 main classes that you will use have the names MainTask1 & MainTask2. Please don’t change the name of those files. It’s the main function of those files that will be used to run your application. 


## Visualization
* Connect to the superset dashboard of your team, and import the .zip file containing the dashboard template of the part 1 "Hands-On".

