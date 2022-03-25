
# NovHack 2022 Development Environment

This is the development environment that your group can use during the NovHack 2022.
Use this space to try out, explore, develop and finally submit your solution.

/!\ Fork this repository in your own github account and give other member of your group the rights to read/write in that repository.

## Setup
1. Check if you have the correct java version 

```
java --version
```
If java is not installed or if the version is below OpenJDK 11, 
download the latest version. https://www.oracle.com/be/java/technologies/javase/jdk11-archive-downloads.html

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
* Create the jar dependencies for Flink
* Download the historical data

4. Configure your IDE to run the code. The detailed instructions are listed
here https://docs.google.com/document/d/1XDWkBU49dmF5JBVskEHGcslIvPFNTpkvZEgD2Rk2c48/edit?usp=sharing


## Submit code

You should have bash script file called 
```submit.sh```

This script will package your application into a .jar file and send it directly to AWS where we will get your files. As in the assignment, two main tasks are asked, the file will automatically send the 2 requested jar. 

:warning: The submit.sh expect that the 2 main classes that you will use have the names MainTask1 & MainTask2. Please don’t change the name of those files. It’s the main function of those file that will be used to run your application. 

 
:warning: Also In MainTask1, you will find the following line at the beginning of the main. 
```
//        Properties props = getKdaApplicationProperties().get("submissionProperties");
       Properties props = getLocalApplicationProperties().get("submissionProperties");
```
Please **uncomment** the first line and **comment** on the second before submitting your code. 


### Common Issues

*What I type is not what I expect*

Make sure to have the latest version of your web browser installed ([Reference](https://community.gitpod.io/t/terminal-keyboard-language-layout/4852)).

## Visualization
* Connect to the superset dashboard of your team, and import the .zip file containing the dashboard template of the part 1 "Hands-On".

