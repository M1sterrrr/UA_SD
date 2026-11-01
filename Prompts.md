Prompt: i attached the pdf for my distributed systems project (Practica_SD2627_WaterManagement_Vfinal_EN.pdf). read it and make me a roadmap in markdown, small steps split into phases. im doing it in python. explain stuff before each step because i have to defend all of it at the exam. no pytest, ill just test by running it
Prompt: whats the difference between a monitor and an engine here? why does the station need 2 separate apps
Prompt: what does [project.scripts] actually do in pyproject
Prompt: why cant i just do sock.send(“AUTH#WS2”) and sock.recv() and get the message? why do i need the stx etx thing
Prompt: explain lrc to me like im 5
Prompt: ok write protocol.py with the frames and ack/nack, max 3 retries like the pdf says
Prompt: why StrEnum and not a normal Enum
Prompt: whats @dataclass for, couldnt i just use a dict
Prompt: next step: central socket server, one thread per monitor. first message has to be AUTH#ws_id and central checks it in sqlite. also a seed script with some stations and operators (use some places from krakow)
Prompt: can 2 threads use the same sqlite connection or will it break
Prompt: sometimes the monitor gets the wrong reply from central, i think the ping thread and the health thread send at the same time on the same socket. how do i fix that
Prompt: if i pull out the network cable central never notices the monitor is gone. how do i fix that
Prompt: give me a docker compose with just kafka for local testing, no zookeeper
Prompt: i dont get the listeners part, why localhost:9092 for me and kafka:19092 for containers
Prompt: help me plan the kafka topics before i code anything. which app writes and reads what? and i dont want old START commands coming back after a restart
Prompt: now central has to answer irrigation requests: RECEIVED, check operator and duration, check the station is AVAILABLE, then START and AUTHORIZED, or DENIED with a reason
Prompt: central just freezes after the first request, no error nothing
Prompt: engine watering now: send telemetry every second and FINISHED at the end with why it ended (time, stop, fault, blocked)
Prompt: whats the difference between event.wait(1) and time.sleep(1)
Prompt: how does the FO know which messages are for him if everyone reads the same topic
Prompt: the first START after i start the engine sometimes just disappears?? the second one works fine
Prompt: the dashboard buttons sometimes dont do anything when i click them
Prompt: explain event delegation
Prompt: make dockerfiles for each module with uv and a compose file that runs everything
Prompt: central crashes in docker because kafka isnt ready yet. depends_on doesnt seem to help?
Prompt: whats the difference between a docker image and a container
Prompt: i was gonna use railway but turns out it doesn’t work so we have to follow the aws tutorial pdf from class, wrrite the deploy.yml
Prompt: ecs doesnt pass arguments, only env variables. how do i make central read them but still work with args locally
Prompt: dashboard works on the ecs link but my monitors cant connect to port 5500. why??
Prompt: if kafka isnt up when central starts, central keeps running but doesnt answer anything. also the terminal gets spammed with kafka errors
Prompt: the correction guide says FO should see its results after a restart. how do i do that
Prompt: im preparing for the exam. explain one full irrigation from start to end, what goes over sockets and what over kafka. then ask me questions like what happens if the monitor dies and correct me
Prompt: go through the whole repo and add polish comments to every function, what it does and how it connects to the rest
