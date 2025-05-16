package com.mysite.sbb.subscript;

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
    private String PAN_ID;

    private int RNUM;
    private String UPP_AIS_TP_NM;
    private String AIS_TP_CD_NM;
    private String PAN_NM;
    private String CNP_CD_NM;
    private String PAN_SS;
    private String DTL_URL;
    private String PAN_NT_ST_DT;
    private String CLSG_DT;

    private String ALL_CNT;
//    private String SPL_INF_TP_CD;
//    private String AIS_TP_CD;
//    private String PAN_DT;
//    private String CCR_CNNT_SYS_DS_CD;
//    private String UPP_AIS_TP_CD;
//    private String DTL_URL_MOB;
}