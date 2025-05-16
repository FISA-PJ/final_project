package com.mysite.sbb.subscript;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

@Service
public class SubscriptService {

    public Page<Subscript> fetchSubscripts(String serviceKey, int pageSize, int pageNum) {
        try {
            // URL 생성
            StringBuilder urlBuilder = new StringBuilder("http://apis.data.go.kr/B552555/lhLeaseNoticeInfo1/lhLeaseNoticeInfo1");
            urlBuilder.append("?" + URLEncoder.encode("serviceKey", "UTF-8") + "=" + serviceKey);
            urlBuilder.append("&" + URLEncoder.encode("PG_SZ", "UTF-8") + "=" + URLEncoder.encode(String.valueOf(pageSize), "UTF-8"));
            urlBuilder.append("&" + URLEncoder.encode("PAGE", "UTF-8") + "=" + URLEncoder.encode(String.valueOf(pageNum), "UTF-8"));
            urlBuilder.append("&" + URLEncoder.encode("UPP_AIS_TP_CD", "UTF-8") + "=" + URLEncoder.encode("05", "UTF-8"));

            // HTTP 요청
            URL url = new URL(urlBuilder.toString());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Content-type", "application/json");

            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(
                            conn.getResponseCode() >= 200 && conn.getResponseCode() <= 300
                                    ? conn.getInputStream()
                                    : conn.getErrorStream()
                    )
            );

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                sb.append(line);
            }
            rd.close();
            conn.disconnect();

            // JSON 파싱
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(sb.toString());

            // data[1].dsList 가져오기
            JsonNode dataArray = root.get(1).path("dsList");

            List<Subscript> results = new ArrayList<>();
            for (JsonNode item : dataArray) {
                Subscript ann = objectMapper.treeToValue(item, Subscript.class);
                results.add(ann);
            }

            // 전체 건수 가져오기 (ALL_CNT)
            int totalElements = 0;
            try {
                totalElements = Integer.parseInt(dataArray.get(0).path("ALL_CNT").asText());
            } catch (Exception e) {
                totalElements = results.size(); // fallback
            }

            // Page 생성
            return new PageImpl<>(results, PageRequest.of(pageNum-1, pageSize), totalElements);

        } catch (Exception e) {
            e.printStackTrace();
            return Page.empty();
        }
    }
}
