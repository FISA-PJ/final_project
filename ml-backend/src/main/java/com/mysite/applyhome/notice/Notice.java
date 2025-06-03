package com.mysite.applyhome.notice;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;

@Entity
@Getter
@Setter
@Table(name = "notices")
public class Notice {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "notice_number", nullable = false, length = 50)
    private String noticeNumber;

    @Column(name = "notice_title", nullable = false, length = 500)
    private String noticeTitle;

    @Column(name = "post_date", nullable = false)
    private LocalDate postDate;

//    @Column(name = "application_start_date", nullable = false)
//    private LocalDate applicationStartDate;

    @Column(name = "application_end_date", nullable = false)
    private LocalDate applicationEndDate;

    @Column(name = "move_in_date")
    private String moveInDate;

    @Column(name = "location", length = 255)
    private String location;

    @Enumerated(EnumType.STRING)
    @Column(name = "notice_status", nullable = false)
    private NoticeStatus noticeStatus;

    @Column(name = "is_correction")
    private Boolean isCorrection;
}

enum NoticeStatus {
    접수중, 접수마감, 결과발표
} 