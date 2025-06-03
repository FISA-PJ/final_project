package com.mysite.applyhome.notice;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.math.BigDecimal;

@Entity
@Getter
@Setter
@Table(name = "house_types", schema = "notice_db")
public class HouseType {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "notice_id")
    private Long noticeId;

    @Column(name = "house_type", nullable = false, length = 50)
    private String houseType;

    @Column(name = "exclusive_area", precision = 10, scale = 2)
    private BigDecimal exclusiveArea;

    @Column(name = "unit_count")
    private Integer unitCount;

    @Column(name = "avg_price", precision = 15, scale = 2)
    private BigDecimal avgPrice;
}