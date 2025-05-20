package com.mysite.applyhome.subscript;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonNaming(PropertyNamingStrategies.UpperSnakeCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Subscript {
    private String PAN_ID; // 공고코드

    private int RNUM; // 순번
    private String UPP_AIS_TP_NM; // 공고유형명
    private String AIS_TP_CD_NM; // 공고세부유형명
    private String PAN_NM; // 공고명
    private String CNP_CD_NM; // 지역명
    private String PAN_SS; // 공고상태코드
    private String DTL_URL; // 공고상세URL
    private String PAN_NT_ST_DT; // 공고게시일
    private String CLSG_DT; // 공고마감일

    private String ALL_CNT; // 전체조회건수
//    private String SPL_INF_TP_CD;
//    private String AIS_TP_CD; // 공고세부유형코드
//    private String PAN_DT;
//    private String CCR_CNNT_SYS_DS_CD;
//    private String UPP_AIS_TP_CD; // 공고유형코드
//    private String DTL_URL_MOB;
}