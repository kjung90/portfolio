
# mass_md 연동

# PH vitamin  QC 강화 ver.

rm(list=ls())

library(dplyr)
library(DBI)
library(RMySQL)
library(writexl)
library(stringr)
library(ssh)

# SSH 터널링
cmd <- 'ssh::ssh_tunnel(ssh::ssh_connect(host = "dataqi@server4.marketingtool.co.kr:22054", passwd = "dataqi123!"), port = 3396, target = "127.0.0.1:3306")'
pid <- sys::r_background(
  std_out = FALSE,
  std_err = FALSE,
  args = c("-e", cmd)
)

# DB연동
con <- dbConnect(MySQL(), user="dataqi", password="dataqi123!", dbname="shield", host="localhost", port = 3396)

# Table 조회
# dbListTables(con)

# 인코딩 1차
dbSendQuery(con, "SET NAMES utf8")
dbSendQuery(con, "SET CHARACTER SET utf8")
dbSendQuery(con, "SET character_set_connection=utf8;")

# 쿼리날려 테이블 가져오기
df = dbGetQuery(con, 'Select * from mass_md_nielsen_ph_ap_355_402_to where data_match=99')

# 인코딩 2차
Encoding(df[,9]) <- 'UTF-8' # top_title
Encoding(df[,10]) <- 'UTF-8' # middle_title
Encoding(df[,11]) <- 'UTF-8' # botton_title
Encoding(df[,12]) <- 'UTF-8' # below_title
Encoding(df[,18]) <- 'UTF-8' # title
Encoding(df[,19]) <- 'UTF-8' # option_name

# DB연동 종료
dbDisconnect(con)

################## 검수로직 적용 전 메타간 사전 검수 ######################
# 숫자로 형태변환을 시켜줄 칼럼 중 meta값이 두 자릿 수 이상인데 0으로 시작되는 케이스 검수
# meta_pieces, meta_bundles, meta_weight

tag_err <- ifelse(str_extract(df$meta_pieces, "^0[0-9]{0,}"), df$tag_err <- "pieces_err",
                  ifelse(str_extract(df$meta_bundles, "^0[0-9]{0,}"), df$tag_err <- "bundles_err",
                         ifelse(str_extract(df$meta_weight, "^0[0-9]{0,}"), df$tag_err <-"weight_err",
                                ifelse(str_extract(df$meta_grade, "^0[0-9]{0,}"), df$tag_err <-"grade_err", df$tag_err <- "NA"))))

df$tag_err <- tag_err
check_pbw <- df %>% 
  filter(tag_err!="NA")

################## 1. set 값 검수 ######################
# hashcode count
df_key <- df %>% 
  count(hashcode)

# hashcode count와 df 합병
df_merge <- merge(df, df_key, by="hashcode", all.x = TRUE)

# 합병된 데이터 중 set가 2값 이상인 데이터 추출
df_set <- df_merge %>% 
  filter(n>=2)

# 필요한 칼럼만 뽑아 set상품 체크
set_check <- df_set %>% 
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_category, n)

# 정상 시퀀스 제외
# 예외시퀀스 정리파일 불러오기
seq_except <- read.csv("D:/김정민/타고객사/동남아/예외처리/set_seq_Exception_210805.csv")

# unqiue 를 사용해서 벡터의 중복된 값을 제거한다
se <- unique(seq_except$PH.Vitamin)

# 합병하기 위해 mat 구조로 변환
rows_seq <- length(se)
mat_seq <- matrix(nrow=rows_seq, ncol=2)
mat_seq[,1] <- se
mat_seq[,2] <- "O"
colnames(mat_seq) <- c("seq","OX") # 이름 삽입

# df1_1_result 와 he를 합병 후 he값이 null인 데이터만 추출 = 예외가 안된 데이터
merge_set <- merge(set_check, mat_seq, by="seq", all.x = TRUE)

check_set <- merge_set %>% 
  filter(is.na(OX))

################### 2. meta 값 검수 ######################
# 칼럼명 순서대로 정렬 + 일부 메타값만 숫자형으로 치환
df1_1 <- df %>% 
  select(hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type, meta_grade, meta_category)
df1_1$meta_pieces <- as.numeric(df1_1$meta_pieces)
df1_1$meta_bundles <- as.numeric(df1_1$meta_bundles)
df1_1$meta_weight <- as.numeric(df1_1$meta_weight)
df1_1$meta_grade <- as.numeric(df1_1$meta_grade)


# meta 값 검수
# node 검수
tag_node <- ifelse(df1_1$middle_title != df1_1$bottom_title, tag_node <-"O", tag_node <- "")
df1_1$tag_node <- tag_node

# pieces가 100초과거나 0인 값
tag_pieces <-  ifelse(df1_1$meta_pieces > 100 | df1_1$meta_pieces==0, tag_pieces <- "O", tag_pieces <- "")
df1_1$tag_pieces <- tag_pieces

# bundles가 9999초과거나 0인 값
tag_bundles <-  ifelse(df1_1$meta_bundles > 9999 | df1_1$meta_bundles==0 , tag_bundles <- "O", tag_bundles <- "")
df1_1$tag_bundles <- tag_bundles

# meta_weight
# meta_weight_type이 1이면서 meta_weight가 1000 이상인 값
# meta_weight_type이 2이면서 meta_weight가 30 초과인 값
# meta_weight_type이 3이면서 meta_weight가 5000 초과인 값
# meta_weight_type이 4이면서 meta_weight가 1000 이상인 값
# meta_weight_type이 9이면서 meta_weight가 5 이상인 값
# meta_weight_type이 9999 이면서 meta_weight 가 9999가 아닌 값
# meta_weight_type이 9999 가 아니면서 meta_weight 가 9999인 값
tag_weight <-  ifelse(df1_1$meta_weight_type=="1" & df1_1$meta_weight >= 1000 | df1_1$meta_weight_type=="2" & df1_1$meta_weight > 30 | 
                        df1_1$meta_weight_type=="3" & df1_1$meta_weight > 5000 | df1_1$meta_weight_type=="4" & df1_1$meta_weight >= 1000 | 
                        df1_1$meta_weight_type=="9" & df1_1$meta_weight >= 5 | df1_1$meta_weight_type=="9999" & df1_1$meta_weight!=9999 | 
                        df1_1$meta_weight_type!="9999" & df1_1$meta_weight==9999, tag_weight <- "O", tag_weight <- "")
df1_1$tag_weight <- tag_weight

# meta_weight_type이 1,2,3,4,9,9999 외 값
tag_weight_type <- ifelse(df1_1$meta_weight_type!="1" & df1_1$meta_weight_type!="2" & df1_1$meta_weight_type!="3" & df1_1$meta_weight_type!="4" & 
                            df1_1$meta_weight_type!="9" & df1_1$meta_weight_type!="9999", tag_weight_type <- "O", tag_weight_type <- "")
df1_1$tag_weight_type <- tag_weight_type

# meta_type 이 1~6,9999 외 값
tag_type <- ifelse(df1_1$meta_type!="1" & df1_1$meta_type!="2" & df1_1$meta_type!="3" & df1_1$meta_type!="4" & df1_1$meta_type!="5" & df1_1$meta_type!="6" & df1_1$meta_type!="9999", tag_type <- "O", tag_type <- "")
df1_1$tag_type <- tag_type

# meta_grade 이 9999 초과거나 0인 값
tag_grade <- ifelse(df1_1$meta_grade > 9999 | df1_1$meta_grade==0, tag_grade <- "O", tag_grade <- "")
df1_1$tag_grade <- tag_grade

# meta_category 이 1,2,9999 외 인 값
tag_category <- ifelse(df1_1$meta_category!="1" & df1_1$meta_category!="2" & df1_1$meta_category!="9999" , tag_category <- "O", tag_category <- "")
df1_1$tag_category <- tag_category

# meta값들 중 na값 존재여부
tag_na <- ifelse(is.na(df1_1$meta_pieces) | is.na(df1_1$meta_bundles) | is.na(df1_1$meta_weight) | df1_1$meta_weight_type=="NA" | df1_1$meta_type=="NA" | is.na(df1_1$meta_grade) |  df1_1$meta_category=="NA" , tag_na <- "O", tag_na <- "")
df1_1$tag_na <- tag_na

# 오데이터만 추출
df1_1_result <- df1_1 %>% 
  filter(tag_node=="O" | tag_pieces=="O" | tag_bundles=="O" | tag_weight=="O" | tag_weight_type=="O" | tag_type=="O" | tag_grade=="O" | tag_category=="O" | tag_na=="O")

# 정상해시 제외
# 예외해시 정리파일 불러오기
hash_except <- read.csv("D:/김정민/타고객사/동남아/예외처리/Meta_Hashcode_Exception_210902.csv")

# unqiue 를 사용해서 벡터의 중복된 값을 제거한다
he <- unique(hash_except$TH.Hair.Care)

# 합병하기 위해 mat 구조로 변환
rows_hash <- length(he)
mat_hash <- matrix(nrow=rows_hash, ncol=2)
mat_hash[,1] <- he
mat_hash[,2] <- "O"
colnames(mat_hash) <- c("hashcode","OX") # 이름 삽입

# df1_1_result 와 he를 합병 후 he값이 null인 데이터만 추출 = 예외가 안된 데이터
merge_meta <- merge(df1_1_result, mat_hash, by="hashcode", all.x = TRUE)

check_meta <- merge_meta %>% 
  filter(is.na(OX))

# 추출된 meta 이상치 리스트에서 각 각의 메타값 확인

df1_1 %>% count(meta_pieces)
df1_1 %>% count(tag_pieces)

df1_1 %>% count(meta_bundles)
df1_1 %>% count(tag_bundles)

df1_1 %>% count(meta_weight)
df1_1 %>% count(tag_weight)

df1_1 %>% count(meta_weight_type)
df1_1 %>% count(tag_weight_type)

df1_1 %>% count(meta_type)
df1_1 %>% count(tag_type)

df1_1 %>% count(meta_grade)
df1_1 %>% count(tag_grade)

df1_1 %>% count(meta_category)
df1_1 %>% count(tag_category)

df1_1 %>% count(tag_na)


################### 3. 하위 패턴 값 검수 ######################
# 1) meta_pieces 패턴 검수

# 패턴 count
df_pat_p <- df %>% 
  count(meta_pieces)

# 패턴 count 순서대로 나열
df_pat_sort_p <- df_pat_p[c(order(df_pat_p$n)),] # 작은값 순서 추출

# 행 번호 초기화
rownames(df_pat_sort_p) <- NULL

# count 누적 합 나타낼 열 추가 후 합병
rows_p <- nrow(df_pat_sort_p)

mat_p <- matrix(nrow=rows_p, ncol=1)

for (row in 1:rows_p){
  if(row==1){
    mat_p[row,1] <-df_pat_sort_p$n[row]
  }else {
    mat_p[row,1] <-df_pat_sort_p$n[row] + mat_p[row-1,1]
  }
}

df_pat_sort_final_p <- cbind(df_pat_sort_p, mat_p)

# 하위 1% count 값 추출
cnt_p <- sum(df_pat_p$n)*0.01

# 하위 패턴의 합이 112 이하인 값 필터링 후 해당 패턴 추출
df_pat_result_p <- df_pat_sort_final_p %>% 
  filter(mat_p <= cnt_p)

pat_p <- df_pat_result_p[,(1:2)]

# df에서 pat에 들어있는 숫자만 뽑고 필요한 칼럼만 추출
pat_merge_p <- merge(df, pat_p, by="meta_pieces", all.x=TRUE)

pat_pieces <- pat_merge_p %>% 
  filter(n!="NA") %>% 
  select(hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type)

# 태그값 생성
pat_pieces$tag_count <- "tag_pieces_count"

# 2) meta_bundles 패턴 검수

# 패턴 count
df_pat_b <- df %>% 
  count(meta_bundles)

# 패턴 count 순서대로 나열
df_pat_sort_b <- df_pat_b[c(order(df_pat_b$n)),] # 작은값 순서 추출

# 행 번호 초기화
rownames(df_pat_sort_b) <- NULL

# count 누적 합 나타낼 열 추가 후 합병
rows_b <- nrow(df_pat_sort_b)

mat_b <- matrix(nrow=rows_b, ncol=1)

for (row in 1:rows_b){
  if(row==1){
    mat_b[row,1] <-df_pat_sort_b$n[row]
  }else {
    mat_b[row,1] <-df_pat_sort_b$n[row] + mat_b[row-1,1]
  }
}

df_pat_sort_final_b <- cbind(df_pat_sort_b, mat_b)

# 하위 1% count 값 추출
cnt_b <- sum(df_pat_b$n)*0.01

# 하위 패턴의 합이 112 이하인 값 필터링 후 해당 패턴 추출
df_pat_result_b <- df_pat_sort_final_b %>% 
  filter(mat_b <= cnt_b)

pat_b <- df_pat_result_b[,(1:2)]

# df에서 pat에 들어있는 숫자만 뽑고 필요한 칼럼만 추출
pat_merge_b <- merge(df, pat_b, by="meta_bundles", all.x=TRUE)

pat_bundles <- pat_merge_b %>% 
  filter(n!="NA") %>% 
  select(hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type)

# 태그값 생성
pat_bundles$tag_count <- "tag_bundles_count"

# 3) meta_weight & meta_weight_type 패턴 검수

# 패턴 count
df_pat_wwt <- df %>% 
  group_by(meta_weight, meta_weight_type) %>% 
  count()

# 패턴 count 순서대로 나열
df_pat_sort_wwt <- df_pat_wwt[c(order(df_pat_wwt$n)),] # 작은값 순서 추출

# 행 번호 초기화
rownames(df_pat_sort_wwt) <- NULL

# count 누적 합 나타낼 열 추가 후 합병
rows_wwt <- nrow(df_pat_sort_wwt)

mat_wwt <- matrix(nrow=rows_wwt, ncol=1)

for (row in 1:rows_wwt){
  if(row==1){
    mat_wwt[row,1] <-df_pat_sort_wwt$n[row]
  }else {
    mat_wwt[row,1] <-df_pat_sort_wwt$n[row] + mat_wwt[row-1,1]
  }
}

df_pat_sort_final_wwt <- cbind(df_pat_sort_wwt, mat_wwt)

# 하위 1% count 값 추출
cnt_wwt <- sum(df_pat_wwt$n)*0.01

# 하위 패턴의 합이 112 이하인 값 필터링 후 해당 패턴 추출
df_pat_result_wwt <- df_pat_sort_final_wwt %>% 
  filter(...4 <= cnt_wwt)

pat_wwt <- df_pat_result_wwt[,(1:3)]

# df에서 pat에 들어있는 숫자만 뽑고 필요한 칼럼만 추출
pat_merge_wwt <- merge(df, pat_wwt, by=c("meta_weight","meta_weight_type"), all.x=TRUE)

pat_w_wt <- pat_merge_wwt %>% 
  filter(n!="NA") %>% 
  select(hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type)

# 태그값 생성
pat_w_wt$tag_count <- "tag_w_wt_count"

# 하위패턴 데이터 전체 합치고 추출
check_pat <- rbind(pat_pieces, pat_bundles, pat_w_wt)


################### 3. 키워드 검수 ######################

# 1. gram 검수

# 필요한 칼럼만 추출
df1 <- df %>% 
  select(hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type)

# 타이틀 옵션 공백 제거
df1$title_1 <- gsub(" ","",df1$title)
df1$option_name_1 <- gsub(" ","",df1$option_name)

# weight_type 이 g 인 데이터
df1_g <- df1 %>% 
  filter(meta_weight_type=="4")

# title & option에서(옵션우선) 0~9 사이의 숫자 1개 이상 4개 이하에 gram,gms,gr,g,GRAM,GR,Gr,G 등의 패턴으로 끝나는 문자열 추출 (긴문자열부터 추출)
a <- ifelse(is.na(str_extract(df1_g$option_name_1,"[1-9][0-9]{0,3}+gram|[1-9][0-9]{0,3}+gms|[1-9][0-9]{0,3}+gr|[1-9][0-9]{0,3}+g|[1-9][0-9]{0,3}+GRAM|[1-9][0-9]{0,3}+GR|[1-9][0-9]{0,3}+Gr|[1-9][0-9]{0,3}+G|[1-9][0-9]{0,3}+กรัม")),
            a <- str_extract(df1_g$title_1,"[1-9][0-9]{0,3}+gram|[1-9][0-9]{0,3}+gms|[1-9][0-9]{0,3}+gr|[1-9][0-9]{0,3}+g|[1-9][0-9]{0,3}+GRAM|[1-9][0-9]{0,3}+GR|[1-9][0-9]{0,3}+Gr|[1-9][0-9]{0,3}+G|[1-9][0-9]{0,3}+กรั"),
            a <- str_extract(df1_g$option_name_1,"[1-9][0-9]{0,3}+gram|[1-9][0-9]{0,3}+gms|[1-9][0-9]{0,3}+gr|[1-9][0-9]{0,3}+g|[1-9][0-9]{0,3}+GRAM|[1-9][0-9]{0,3}+GR|[1-9][0-9]{0,3}+Gr|[1-9][0-9]{0,3}+G|[1-9][0-9]{0,3}+กรั")
)

# df1_g 에 합병
df1_g_a <- cbind(df1_g, a)

# 2. liter 검수 

# weight_type 이 l 인 데이터
df1_l <- df1 %>% 
  filter(meta_weight_type=="2")

# title & option에서(옵션우선) 0~9 사이의 숫자 1개 이상 4개 이하에 gram,gms,gr,g,GRAM,GR,Gr,G 등의 패턴으로 끝나는 문자열 추출 (긴문자열부터 추출)
a <- ifelse(is.na(str_extract(df1_l$option_name_1,"\\d+liter|\\d+litre|\\d+l|\\d+L|\\d+Liter|\\d+Litre|\\d.\\d+litre|\\d.\\d+liter|\\d.\\d+l|\\d.\\d+Litre|\\d.\\d+Liter|\\d.\\d+L|\\d.\\d+ล")),
            a <- str_extract(df1_l$title_1,"\\d+liter|\\d+litre|\\d+l|\\d+L|\\d+Liter|\\d+Litre|\\d.\\d+litre|\\d.\\d+liter|\\d.\\d+l|\\d.\\d+Litre|\\d.\\d+Liter|\\d.\\d+L|\\d.\\d+ล"),
            a <- str_extract(df1_l$option_name_1,"\\d+liter|\\d+litre|\\d+l|\\d+L|\\d+Liter|\\d+Litre|\\d.\\d+litre|\\d.\\d+liter|\\d.\\d+l|\\d.\\d+Litre|\\d.\\d+Liter|\\d.\\d+L|\\d.\\d+ล")
)

# df1_l 에 합병
df1_l_a <- cbind(df1_l, a)

# g, l 합병 후 a가 NA 값만 추출
check_keyword <- rbind(df1_g_a, df1_l_a) %>% 
  filter(is.na(a))


# write_xlsx(check_set, "D:/김정민/타고객사/동남아/mass_md_태국/Vitamin/P&G_TH_vitamin_setcheck_result_1007_1.xlsx")
write_xlsx(df1_1_result, "C:/Users/user/Desktop/업무/동남아/PH_vitamin_metacheck_result_1224.xlsx")
write_xlsx(check_pat, "C:/Users/user/Desktop/업무/동남아/PH_vitamin_pattern_check_result_1224.xlsx")
write_xlsx(check_keyword, "C:/Users/user/Desktop/업무/동남아/PH_vitamin_keyword_check_result_1224.xlsx")


