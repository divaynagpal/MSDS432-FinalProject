This program has 1 microservice that has 8 routes:
1. <ip>:8080/report1
2. <ip>:8080/report2
3. <ip>:8080/report3
4. <ip>:8080/report4a
5. <ip>:8080/report4b
6. <ip>:8080/report4c
7. <ip>:8080/report5
8. <ip>:8080/report6

For importing the code you can use any ode of your choice. I used visual studio to build. You can import the folder as is in your Visual studio workspace. 

For installing the api, follow the below steps.
1. build your docker image -> docker build -t my-go-app .

2. copy tag of docker image -> docker tag my-go-app:latest divay1989/ms432projectrepo:latest

3. login to docker hub from terminal -> docker login

4. Go to docker desktop and you can see your image there. Click on push to hub for your image and your image will be pushed.

5. Go to compute engine in GCP, create/start your Virtual Machine instance and ssh on it.

6. Download image in VM instance - sudo docker pull divay1989/ms432projectrepo:latest

7. Check the image is downloaded from docker hub -> sudo docker images

8. Run the image -> sudo docker run -d -p 8080:8080 divay1989/ms432projectrepo:latest

9. You should see a message like "Ready to serve at :8080"

10. Go to chrome and hit the url http://<ip>:8080/report1. You should see a json printed on screen
