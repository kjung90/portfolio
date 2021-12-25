# mass_md petfood_고양이사료


# SSH 터널링
cmd <- 'ssh::ssh_tunnel(ssh::ssh_connect(host = "xxx@xxx.co.kr:00000", passwd = "xxxxx"), port = xxxx, target = "000.0.0.0:0000")'
pid <- sys::r_background(
  std_out = FALSE,
  std_err = FALSE,
  args = c("-e", cmd)
)

# DB연동
con <- dbConnect(MySQL(), user="xxxx", password="xxxx!", dbname="xxxx", host="localhost", port = 0000)

# Table 조회
# dbListTables(con)

# 인코딩 1차
dbSendQuery(con, "SET NAMES utf8")
dbSendQuery(con, "SET CHARACTER SET utf8")
dbSendQuery(con, "SET character_set_connection=utf8;")

# 쿼리날려 테이블 가져오기
df = dbGetQuery(con, 'Select * from xxxx')

# 인코딩 2차
Encoding(df[,9]) <- 'UTF-8' # top_title
Encoding(df[,10]) <- 'UTF-8' # middle_title
Encoding(df[,11]) <- 'UTF-8' # botton_title
Encoding(df[,12]) <- 'UTF-8' # below_title
Encoding(df[,18]) <- 'UTF-8' # title
Encoding(df[,19]) <- 'UTF-8' # option_name

# DB연동 종료
dbDisconnect(con)


################# 필요한 칼럼만 사용####################
df1 <- df %>%
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, sm_active, multi_meta, multi_meta_json, multi_meta_count, meta_box, meta_size, meta_type, meta_breed, meta_combo, meta_grade, meta_pieces, meta_sample,
         meta_weight, meta_bundles, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, meta_weight_type) %>%
  filter(sm_active=="1")


################## 1. set 값 검수 ######################
# hashcode count
df_key <- df1 %>%
  count(hashcode)

# hashcode count와 df 합병
df_merge <- merge(df1, df_key, by="hashcode", all.x = TRUE)

# 합병된 데이터 중 set가 2값 이상인 데이터 추출
df_set <- df_merge %>%
  filter(n>=2)

# 필요한 칼럼만 뽑아 set상품 체크
check_set <- df_set %>%
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_json, multi_meta_count, meta_pieces, meta_bundles, meta_weight, 
         meta_weight_type, meta_type, meta_grade, meta_box, meta_sample, meta_size, meta_breed,         meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, n)

# set상품이 맞을 시 seq 기준으로 제거 (같은 해시가 새로생긴 해시에 들어갈 경우 예외처리되면 문제가 될 수 있음)
check_set1 <- check_set %>%
  filter(
  )


################## 2. multi meta 검수 ######################

# 2-1. 멀티메타 값 풀기
# 작업이 많아 multi_meta_json에 값이 있는 것들만 추출
# df %>% count(multi_meta_count)
df2 <- df1 %>%
  filter(multi_meta_count>=2)

# json 칼럼만 추출
mtjsn <- df2$multi_meta_json

# 필요없는 문자열 치환(제거)
mtjsn_1 <- str_replace_all(mtjsn,pattern = '"', replacement = '')
mtjsn_11 <- gsub("taste_1", "taste_a", mtjsn_1)
mtjsn_12 <- gsub("taste_2", "taste_b", mtjsn_11)
mtjsn_13 <- gsub("taste_3", "taste_c", mtjsn_12)
mtjsn_14 <- gsub("taste_4", "taste_d", mtjsn_13)
mtjsn_15 <- gsub("taste_5", "taste_e", mtjsn_14)
mtjsn_16 <- gsub("meta_set", "", mtjsn_15)
mtjsn_17 <- gsub("multi_meta_json", "", mtjsn_16)
mtjsn_18 <- gsub("multi_meta_count", "", mtjsn_17)


# 콤마(,)로 구분하여 인덱스 생성
mtjsn_2 <- strsplit(mtjsn_18, ",")
mtjsn_2[[71]]

# 값 넣을 dataframe 생성
df_multi <- data.frame(matrix(nrow=0,ncol=0))

mtjsn_2[[7]]

# 문자열에서 숫자만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_", mtjsn_2[[i]]) # meta_ 가 들어가는 위치 추출 후 반복문에 삽입
  k <- 1 # k 값 1로 복구
  for(j in jj){
    df_multi[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1 # k는 1씩 증가한다
  }
}

# df2에 dataframe 합치기
df3 <- cbind(df2[,c(1:8)],df2$multi_meta_count, df_multi)

# 칼럼이름 바꿔주기
colnames(df3)[9] <- "multi_meta_count"

# 값 묶어주기
df4 <- df3 %>%
  mutate(cols1=paste(V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,V11,V12,V13,V14,V15,V16, sep=",")) %>%
  mutate(cols2=paste(V17,V18,V19,V20,V21,V22,V23,V24,V25,V26,V27,V28,V29,V30,V31,V32, sep=",")) %>%
  mutate(cols3=paste(V33,V34,V35,V36,V37,V38,V39,V40,V41,V42,V43,V44,V45,V46,V47,V48, sep=",")) %>%
  mutate(cols4=paste(V49,V50,V51,V52,V53,V54,V55,V56,V57,V58,V59,V60,V61,V62,V63,V64, sep=",")) %>%
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_count, cols1, cols2, cols3, cols4)

# 값 넣을 dataframe 생성
df5 <- data.frame(matrix(nrow=0, ncol=0))

# 값별로 행 늘려주기
for (loop_col in 10:13){
  input_col <- df4[,c(1:9,loop_col)]
  names(input_col)[10] <- "cols"
  df5 <- rbind(df5, input_col)
}

# cols 칼럼이 NA인 값 제거
df6 <- df5 %>%
  filter(cols!="NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA")

# 푼 다중메타 수와 멀티메타카운트의 합 일치여부 확인
sum(df2$multi_meta_count) == nrow(df6)

# 콤마로 묶인 값 펼치기
spl <- str_split(df6$cols, ",")
# spl[[1]][1]

# 펼친 값 넣을 데이터 프레임 생성
df7 <- data.frame(matrix(nrow=nrow(df6),ncol=16))

# 펼친 값 다시 넣어주기
for (i in 1:nrow(df6)){
  for (j in 1:16){
    df7[i,j] <- spl[[i]][j]
  }
}

# data 합쳐주기
df8 <- cbind(df6[,c(1:9)], df7)

# 칼럼에 이름 넣어주기
colnames(df8)[10] <- "meta_box"
colnames(df8)[11] <- "meta_size"
colnames(df8)[12] <- "meta_type"
colnames(df8)[13] <- "meta_breed"
colnames(df8)[14] <- "meta_combo"
colnames(df8)[15] <- "meta_grade"
colnames(df8)[16] <- "meta_pieces"
colnames(df8)[17] <- "meta_sample"
colnames(df8)[18] <- "meta_weight"
colnames(df8)[19] <- "meta_bundles"
colnames(df8)[20] <- "meta_taste_1"
colnames(df8)[21] <- "meta_taste_2"
colnames(df8)[22] <- "meta_taste_3"
colnames(df8)[23] <- "meta_taste_4"
colnames(df8)[24] <- "meta_taste_5"
colnames(df8)[25] <- "meta_weight_type"

# 2-2. 멀티메타 이상여부 검수
# 1. multi_meta_count가 1인 값 체크
multi_check_1 <- df1 %>%
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, sm_active, multi_meta, multi_meta_json, multi_meta_count, meta_box, meta_size, meta_type, meta_breed, meta_combo, meta_grade, meta_pieces, meta_sample,
         meta_weight, meta_bundles, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, meta_weight_type) %>%
  filter(multi_meta_count=="1")

# 2. multi_meta 모든 값이 같은 데이터 확인
# 모든 메타 조합으로 key값 생성
dataf <- df8 %>%
  mutate(key=paste(hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_count, meta_box, meta_size, meta_type, meta_breed, meta_combo, meta_grade, meta_pieces, meta_sample,
                   meta_weight, meta_bundles, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, meta_weight_type, sep="|"))

# key 카운트
dataf_cnt <- dataf %>%
  count(key)

# key값 기준 합병
merge_multi <- merge(dataf, dataf_cnt, by="key", all.x = TRUE)

# key가 2개 이상인 값 추출
multi_check_2 <- merge_multi %>%
  filter(n>1)



################## 3. meta 값 검수 ######################
# 칼럼명 순서대로 정렬 + 일부 메타값만 숫자형으로 치환
df1_1 <- df1 %>%
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_count, meta_box, meta_size, meta_type, meta_breed, meta_combo, meta_grade, meta_pieces, meta_sample,
         meta_weight, meta_bundles, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, meta_weight_type)

# 다중메타 데이터 제거 후 2에서 생성한 df8 rbind 하기
df1_2 <- df1_1 %>%
  filter(multi_meta_count<=1)

# 합친 후 칼럼 정렬 (meta_combo 필요없으므로 제거)
df1_3 <- rbind(df1_2, df8) %>%
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_count, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type, meta_grade, meta_box, meta_sample, meta_size, meta_breed, meta_taste_1
         , meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5)


# meta값 숫자형으로 변환
df1_3$meta_pieces <- as.numeric(df1_3$meta_pieces)
df1_3$meta_bundles <- as.numeric(df1_3$meta_bundles)
df1_3$meta_weight <- as.numeric(df1_3$meta_weight)
df1_3$meta_size <- as.numeric(df1_3$meta_size)
df1_3$meta_breed <- as.numeric(df1_3$meta_breed)
df1_3$meta_taste_1 <- as.numeric(df1_3$meta_taste_1)
df1_3$meta_taste_2 <- as.numeric(df1_3$meta_taste_2)
df1_3$meta_taste_3 <- as.numeric(df1_3$meta_taste_3)
df1_3$meta_taste_4 <- as.numeric(df1_3$meta_taste_4)
df1_3$meta_taste_5 <- as.numeric(df1_3$meta_taste_5)

# meta 값 검수
# pieces가 9999보다 크거나 0인 값
tag_pieces <-  ifelse(df1_3$meta_pieces > 9999 | df1_3$meta_pieces==0, tag_pieces <- "O", tag_pieces <- "")
df1_3$tag_pieces <- tag_pieces

# bundles가 9999보다 크거나 0인 값
tag_bundles <-  ifelse(df1_3$meta_bundles > 9999 | df1_3$meta_bundles==0 , tag_bundles <- "O", tag_bundles <- "")
df1_3$tag_bundles <- tag_bundles

# meta_weight
# meta_weight_type이 1이면서 meta_weight가 1000이상인 값
# meta_weight_type이 2이면서 meta_weight가 100이상인 값
# meta_weight_type이 3이면서 meta_weight가 1000이상인 값
# meta_weight_type이 4이면서 meta_weight가 100이상인 값
tag_weight <-  ifelse(df1_3$meta_weight_type=="1" & df1_3$meta_weight >=1000 | df1_3$meta_weight_type=="2" & df1_3$meta_weight >=100 | df1_3$meta_weight_type=="3" & df1_3$meta_weight >=1000 |df1_3$meta_weight_type=="4" & df1_3$meta_weight >=100,
                      tag_weight <- "O", tag_weight <- "")
df1_3$tag_weight <- tag_weight

# meta_weight_type이 1,2,3,4,5,6,7,9999 외 값
tag_weight_type <- ifelse(df1_3$meta_weight_type!="1" & df1_3$meta_weight_type!="2" & df1_3$meta_weight_type!="3" & df1_3$meta_weight_type!="4" & df1_3$meta_weight_type!="5" & df1_3$meta_weight_type!="6" & df1_3$meta_weight_type!="7" &
                            df1_3$meta_weight_type!="9999", tag_weight_type <- "O", tag_weight_type <- "")
df1_3$tag_weight_type <- tag_weight_type

# meta_type 이 1,2,3 외 값
tag_type <- ifelse(df1_3$meta_type!="1" & df1_3$meta_type!="2" & df1_3$meta_type!="3" ,tag_type <- "O", tag_type <- "")
df1_3$tag_type <- tag_type

# meta_grade 가 1,2,3,4,5,6,7,8,9,9999 외 값
tag_grade <- ifelse(df1_3$meta_grade!="1" & df1_3$meta_grade!="2" & df1_3$meta_grade!="3" & df1_3$meta_grade!="4" & df1_3$meta_grade!="5" & df1_3$meta_grade!="6" & df1_3$meta_grade!="7" & df1_3$meta_grade!="8" & df1_3$meta_grade!="9" &
                      df1_3$meta_grade!="9999", tag_grade <- "O", tag_grade <- "")
df1_3$tag_grade <- tag_grade

# meta_box가 0인 값
tag_box <-  ifelse(df1_3$meta_box=="0", tag_box <- "O", tag_box <- "")
df1_3$tag_box <- tag_box

# meta_sample이 1,9999 외 값
tag_sample <-  ifelse(df1_3$meta_sample!="1" & df1_3$meta_sample!="9999", tag_sample <- "O", tag_sample <- "")
df1_3$tag_sample <- tag_sample

# meta_size가 20이상이며 0, 9999가 아닌 값
tag_size <- ifelse(df1_3$meta_size >=20 & df1_3$meta_size!=9999 & df1_3$meta_size!=0, tag_size <- "O", tag_size <-"")
df1_3$tag_size <- tag_size

# meta_breed가 16이상이며 0,9999가 아닌 값
tag_breed <- ifelse(df1_3$meta_breed >=16 & df1_3$meta_breed!=9999 & df1_3$meta_breed!=0, tag_breed <- "O", tag_breed <-"")
df1_3$tag_breed <- tag_breed

# meta_taste_1가 89이상이며 9999가 아닌 값
tag_taste_1 <- ifelse(df1_3$meta_taste_1 >=89 & df1_3$meta_taste_1!=9999, tag_taste_1 <- "O", tag_taste_1 <- "")
df1_3$tag_taste_1 <- tag_taste_1

# meta_taste_2가 89이상이며 9999가 아닌 값
tag_taste_2 <- ifelse(df1_3$meta_taste_2 >=89 & df1_3$meta_taste_2!=9999, tag_taste_2 <- "O", tag_taste_2 <- "")
df1_3$tag_taste_2 <- tag_taste_2

# meta_taste_3가 89이상이며 9999가 아닌 값
tag_taste_3 <- ifelse(df1_3$meta_taste_3 >=89 & df1_3$meta_taste_3!=9999, tag_taste_3 <- "O", tag_taste_3 <- "")
df1_3$tag_taste_3 <- tag_taste_3

# meta_taste_4가 89이상이며 9999가 아닌 값
tag_taste_4 <- ifelse(df1_3$meta_taste_4 >=89 & df1_3$meta_taste_4!=9999, tag_taste_4 <- "O", tag_taste_4 <- "")
df1_3$tag_taste_4 <- tag_taste_4

# meta_taste_5가 89이상이며 9999가 아닌 값
tag_taste_5 <- ifelse(df1_3$meta_taste_5 >=89 & df1_3$meta_taste_5!=9999, tag_taste_5 <- "O", tag_taste_5 <- "")
df1_3$tag_taste_5 <- tag_taste_5

# meta값들 중 na값 존재여부
tag_na <- ifelse(df1_3$meta_pieces == "NA" | df1_3$meta_bundles == "NA" | is.na(df1_3$meta_weight) | df1_3$meta_weight_type=="NA" | df1_3$meta_type=="NA" | df1_3$meta_grade=="NA" | df1_3$meta_box=="NA" | df1_3$meta_sample=="NA" | df1_3$meta_size=="NA"|
                   is.na(df1_3$meta_breed) | is.na(df1_3$meta_taste_1) | is.na(df1_3$meta_taste_2) | is.na(df1_3$meta_taste_3) | is.na(df1_3$meta_taste_4) | is.na(df1_3$meta_taste_5), tag_na <- "O", tag_na <- "")
df1_3$tag_na <- tag_na

# 오데이터만 추출
df1_3_result <- df1_3 %>%
  filter(tag_pieces=="O" | tag_bundles=="O" | tag_weight=="O" | tag_weight_type=="O" | tag_type=="O" | tag_grade=="O" | tag_box=="O" | tag_sample=="O" | tag_size=="O" | tag_breed=="O" |
           tag_taste_1=="O" | tag_taste_2=="O" | tag_taste_3=="O" | tag_taste_4=="O" | tag_taste_5=="O")

# 값 확인

df1_3 %>% count(meta_pieces)
df1_3 %>% count(tag_pieces)

df1_3 %>% count(meta_bundles)
df1_3 %>% count(tag_bundles)

df1_3 %>% count(meta_weight)
df1_3 %>% count(tag_weight)

df1_3 %>% count(meta_weight_type)
df1_3 %>% count(tag_weight_type)

df1_3 %>% count(meta_type)
df1_3 %>% count(tag_type)

df1_3 %>% count(meta_grade)
df1_3 %>% count(tag_grade)

df1_3 %>% count(meta_box)
df1_3 %>% count(tag_box)

df1_3 %>% count(meta_sample)
df1_3 %>% count(tag_sample)

df1_3 %>% count(meta_size)
df1_3 %>% count(tag_size)

df1_3 %>% count(meta_breed)
df1_3 %>% count(tag_breed)

write_xlsx(check_set, "D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/PetFood_dogfood_DPcheck_result_0615.xlsx")
write_xlsx(multi_check1, "D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/PetFood_dogfood_Multicheck1_result_0615.xlsx")
write_xlsx(multi_check2, "D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/PetFood_dogfood_Multicheck2_result_0615.xlsx")
write_xlsx(df1_3_result, "D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/PetFood_dogfood_metacheck_result_0615.xlsx")
