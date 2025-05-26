USE notice_db;

DELIMITER //

-- 공고 상태 업데이트 트리거
CREATE TRIGGER update_notice_status
BEFORE INSERT ON notices
FOR EACH ROW
BEGIN
    SET @current_date = CURDATE();
    
    -- 공고 상태 판단
    IF NEW.application_end_date IS NULL THEN
        SET NEW.notice_status = '접수중';
    ELSEIF NEW.application_end_date < @current_date THEN
        IF NEW.winning_date IS NOT NULL AND NEW.winning_date <= @current_date THEN
            SET NEW.notice_status = '결과발표';
        ELSE
            SET NEW.notice_status = '접수마감';
        END IF;
    ELSE
        SET NEW.notice_status = '접수중';
    END IF;
END;
//

DELIMITER ;