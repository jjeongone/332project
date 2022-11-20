# [CSED332] SD Team Project: External Merge Sort 

회의록이나 document에 대해서는 다음 [링크](https://jjeongone.notion.site/CSED322-SD-Team-Project-eb2b3814d554467c93a8c22ec02c53d7)에서 확인하실 수 있습니다. `Document` 디렉토리에는 개발 정보가 저장되어 있고, `Meeting Log` 항목에는 주차별 회의록이 정리되어 있습니다.

## Week 1
> 10/21(금) meeting 진행

### Weekly progress
- Github repository 생성
- Notion 생성: documentation 공유를 목적으로 함

### Goal of the week
- project 전반의 flow에 대한 이해
  - external merge sort에 대한 이해
- gRPC를 이용한 worker master 통신 공부
- sampling method 탐색
- Testing 환경 설정에 대한 고찰
- (optional) [databricks scala style guide](https://github.com/databricks/scala-style-guide) 읽어보기

### Goal of the week for each individual member
프로젝트 전반에 대한 이해는 모든 member에게 필수적이기 때문에 각자 *Goal of the week*를 수행하고 다음 meeting에서 공유 및 구체적인 milestone을 세운다.

<br>

## Week 2
> 10/30(일) meeting 진행

### Weekly progress
- project에서 요구하는 기능의 전반적인 흐름에 대해 이해함
- 각 단계별로 master와 worker 사이에서 수행되어야 하는 일과 module간의 관계에 대해 이해함
- 여러 shuffling 방법론에 대한 논의를 나눔
- 이를 바탕으로 앞으로의 milestone에 대한 구체화를 진행함

### Goal of the week
- 각 과정에서 요구되는 자세한 기능과 이를 수행하는 함수 설계를 위한 준비(개념에 대한 이해 및 구체적인 방법론 구현)
- gensort, gRPC와 같이 외부 library를 사용할 경우 해당 tool에 대한 demo를 수행
- coding convention, git convention을 정하여 협업을 함에 있어서의 규칙 정하기
- 개발환경과 testing 환경에 대한 논의를 진행하고, 개발을 시작할 수 있도록 환경 세팅

### Goal of the week for each individual member
|member|goal|
|:---:|---|
|김민결|- pivoting이 잘 되어 있다는 가정 하에 Shuffling method 디자인(10/30 미팅에서 Shuffling을 위한 여러 가지 방법들이 논의되었으나, Shuffling method를 하나로 결정할 필요 있음) <br/> - External Merge Sort 구현|
|이희우|- scalaPB를 이용한 gRPC handler demo 구현 <br/> - github commit convention 정하기|
|최정원|- master에서 사용되는 sampling algorithm 구현 후 pivot 계산을 잘 수행하는지 검증하기 <br/> - test case 설계 및 test environment 구상하기 <br/> - databricks scala style guide를 기반으로 coding convention 정하기|

<br>

# Week 3
> 11/6(일) meeting 진행

### Weekly Progress
- 용어 통일
- gRPC의 전반적인 작동 방식 및 demo 진행상황 공유
- github commit convention 결정
- scala coding style convention 결정
- sampling method 검증 및 전체적인 flow에서 sampling 순서 결정
- test environment 설정에 있어 docker를 사용하기로 결정
- device spec 결정
- 개발환경 통일
  - scala `2.13.8`
- 전체적인 flow 도식화

### Goal of the week
- flow chart 완성
- 개발 시작
- unit test는 개발과 동시진행
- test case 고민
- partitioning 구현

### Goal of the week for each individual member
|member|goal|
|:---:|---|
|김민결|- external merge sort library 실행 <br/> - partitioning 구현|
|이희우|- master, worker간의 asynchronous 방법론 탐색(worker로부터 정보를 모두 받고 master가 연산을 수행) <br/> - message 통신 <br/> - project initiate|
|최정원|- docker를 이용한 master, worker 환경 설정(가장 기본적인 상황을 가정) + partition size 고민 <br/> - sampling algorithm scala 상에 구현 <br/> - gensort로 데이터 생성 및 test 환경에 적용 <br/> - flow chart 정리|

<br>

# Week 4
> 11/13(일) meeting 진행

### Weekly Progress
- java library를 이용하여 external merge sort 구현
- partitioning 구현
- master-worker간 gRPC를 통한 connection 구현 
- gensort를 통한 data set generation
- flow chart 정리
- 가상환경 세팅(진행중)

### Goal of the week
- 각자 local에 VM 세팅
- master와 worker setting을 쉽게 하기 위한 스크립트 작성
- VM 환경에서 external merge sort 작동 여부 확인
- server-client 간 port number를 이용한 통신이 잘 이루어지는지 확인
- gensort로 생성한 file에 대해 sampling algorithm과 pivoting이 잘 작동하는지 확인

### Goal of the week for each individual member
|member|goal|
|:---:|---|
|공통|- progress presentation 발표자료 제작|
|김민결|- VM 환경에서 external sorting library 구동 확인 및 partitioning 검증 <br/> - Flow chart 기반으로 data type 결정|
|이희우|- server에서 여러 client들의 requests를 기다렸다가 한꺼번에 response 처리하는 코드 구현 <br/> - device간 file이나 array 통신 <br/> 각 machine의 port 번호 읽어오는 코드 작성 |
|최정원|- sampling algorithm 및 pivoting 구현 <br/> - VM 개발환경 설정법 document 공유 (~11/14) <br/> - gensort를 이용하여 해당 worker에 요구되는 만큼의 file block 생성하는 스크립트 작성 |

<br>

# Week 5
> 11/20(일) meeting 진행

*앞으로 언급하는 test 환경은 모두 docker container를 의미하며, machine spec은 다음과 같다.*

Ubuntu `22.04`, Java `1.8.0` (openjdk-8), scala `2.13.8`

### Weekly Progress
- gRPC를 이용한 file, array 통신
- machine (master, worker) 내부적으로 IP address 확인하는 코드 구현
- server에서 여러 client의 request를 기다렸다가 한꺼번에 처리하는 코드 구현
- test 환경에서 file block 단위의 sample dataset에 대해 external merge sort 작동 확인
- test 환경에서 임의의 sample dataset에 대해 partitioning 작동 확인
- shell script를 통해 worker container가 실행될 때 2GB의 test data를 gensort로 generation하도록 구현
- type definition 정리

### Goal of the week
- 현재까지 구현된 master, worker 내부 함수에 대한 검증
- Docker를 통해 설정한 test환경이 실제 machine 환경과 유사하게 작동하는지 확인
- 각자 구현한 코드 합치는 작업
- gRPC를 이용한 통신 구현 파트 분배: 다른 부분에 비해 통신이 많은 비중을 차지하고 있기 때문에 우선적으로 automic하게 분배할 수 있도록 역할분배를 하였고, 지금부터는 서로 communication을 통해 적절한 dependency를 가지도록 업무분배를 하여 개발 진행

### Goal of the week for each individual member
|member|goal|
|:---:|---|
|공통|- 지금까지 작업한 부분 git에 branch 생성하여 업로드|
|김민결|- 더 큰 gensort input file에 대해서도 external merge sort가 잘 작동하는지 test <br/> - mergeDone message를 전송함에 있어 요구되는 함수 구현 <br/> - mergeDone message를 수신한 후 master에서 sorting이 잘 이루어졌는지 확인하는 validation 코드 구현|
|이희우|- gRPC를 이용한 file 통신 구현 <br/> - gRPC 통신이 완료된 후 machine termination 구현 <br/> - test 환경에서 IP address를 잘 출력하는지 확인 <br/> - gRPC를 이용한 통신 구현 분배|
|최정원|- type definition을 적용하여 sampling, setPivot 함수 수정 <br/> - test 환경에서 sampling, setPivot 함수 작동 확인 <br/> - Docker network 작동 방식 확인|