package org.superbiz.moviefun.albums;

import org.apache.tomcat.jni.Local;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Configuration
@EnableAsync
@EnableScheduling
public class AlbumsUpdateScheduler {

    private static final long SECONDS = 1000;
    private static final long MINUTES = 60 * SECONDS;

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final AlbumsUpdater albumsUpdater;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public AlbumsUpdateScheduler(AlbumsUpdater albumsUpdater, DataSource dataSource) {
        this.albumsUpdater = albumsUpdater;
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Scheduled(initialDelay = 15 * SECONDS, fixedRate = 2 * MINUTES)
    public void run() {
        try {
//            if (readyToRunUpdate()){
            if (startAlbumSchedulerTask()) {
                logger.debug("Starting albums update");
                albumsUpdater.update();
//                setUpdatedTime();
                logger.debug("Finished albums update");
            } else {
                logger.debug("Nothing to start");
            }
        } catch (Throwable e) {
            logger.error("Error while updating albums", e);
        }
    }

    private boolean readyToRunUpdate() {

        Timestamp lastRunTime = jdbcTemplate.queryForObject("Select started_at from album_scheduler_task LIMIT 1", Timestamp.class);

        if (lastRunTime == null) {
            return true;
        }

        LocalDateTime lastRun = LocalDateTime.ofInstant(lastRunTime.toInstant(), ZoneOffset.UTC);
        System.out.println(lastRun);

        LocalDateTime previousWindow = LocalDateTime.now().minusMinutes(3);
        System.out.println(previousWindow);

        return lastRun.isBefore(previousWindow);
    }

    private void setUpdatedTime() {
        jdbcTemplate.update("Update album_scheduler_task set started_at = now()");
    }

    private boolean startAlbumSchedulerTask() {
        int updatedRows = jdbcTemplate.update(
                "UPDATE album_scheduler_task" +
                        " SET started_at = now()" +
                        " WHERE started_at IS NULL" +
                        " OR started_at < date_sub(now(), INTERVAL 2 MINUTE)"
        );

        return updatedRows > 0;
    }
}
