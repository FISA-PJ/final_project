package com.mysite.applyhome.notice;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@Table(name = "supply_types", schema = "notice_db")
public class SupplyType {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "notice_id")
    private Long noticeId;

    @Column(name = "supply_type", length = 100)
    private String supplyType;
} 