# [CSED332] SD Team Project: External Merge Sort 

회의록이나 document에 대해서는 다음 [링크](https://jjeongone.notion.site/CSED322-SD-Team-Project-eb2b3814d554467c93a8c22ec02c53d7)에서 확인하실 수 있습니다. `Document` 디렉토리에는 개발 정보가 저장되어 있고, `Note` 디렉토리에는 회의록이 정리되어 있습니다.

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

<hr>

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
|||