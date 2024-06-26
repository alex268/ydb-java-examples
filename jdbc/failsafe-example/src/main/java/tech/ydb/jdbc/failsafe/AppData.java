package tech.ydb.jdbc.failsafe;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

final class AppData {
    public static final List<Series> SERIES = Arrays.asList(
        new Series(
            1, "IT Crowd", date("2006-02-03"),
            "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by " +
            "Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
        new Series(
            2, "Silicon Valley", date("2014-04-06"),
            "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and " +
            "Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.")
    );

    public static final List<Season> SEASONS = Arrays.asList(
        new Season(1, 1, "Season 1", date("2006-02-03"), date("2006-03-03")),
        new Season(1, 2, "Season 2", date("2007-08-24"), date("2007-09-28")),
        new Season(1, 3, "Season 3", date("2008-11-21"), date("2008-12-26")),
        new Season(1, 4, "Season 4", date("2010-06-25"), date("2010-07-30")),
        new Season(2, 1, "Season 1", date("2014-04-06"), date("2014-06-01")),
        new Season(2, 2, "Season 2", date("2015-04-12"), date("2015-06-14")),
        new Season(2, 3, "Season 3", date("2016-04-24"), date("2016-06-26")),
        new Season(2, 4, "Season 4", date("2017-04-23"), date("2017-06-25")),
        new Season(2, 5, "Season 5", date("2018-03-25"), date("2018-05-13"))
    );

    public static final List<Episode> EPISODES = Arrays.asList(
        new Episode(1, 1, 1, "Yesterday's Jam", date("2006-02-03")),
        new Episode(1, 1, 2, "Calamity Jen", date("2006-02-03")),
        new Episode(1, 1, 3, "Fifty-Fifty", date("2006-02-10")),
        new Episode(1, 1, 4, "The Red Door", date("2006-02-17")),
        new Episode(1, 1, 5, "The Haunting of Bill Crouse", date("2006-02-24")),
        new Episode(1, 1, 6, "Aunt Irma Visits", date("2006-03-03")),
        new Episode(1, 2, 1, "The Work Outing", date("2006-08-24")),
        new Episode(1, 2, 2, "Return of the Golden Child", date("2007-08-31")),
        new Episode(1, 2, 3, "Moss and the German", date("2007-09-07")),
        new Episode(1, 2, 4, "The Dinner Party", date("2007-09-14")),
        new Episode(1, 2, 5, "Smoke and Mirrors", date("2007-09-21")),
        new Episode(1, 2, 6, "Men Without Women", date("2007-09-28")),
        new Episode(1, 3, 1, "From Hell", date("2008-11-21")),
        new Episode(1, 3, 2, "Are We Not Men?", date("2008-11-28")),
        new Episode(1, 3, 3, "Tramps Like Us", date("2008-12-05")),
        new Episode(1, 3, 4, "The Speech", date("2008-12-12")),
        new Episode(1, 3, 5, "Friendface", date("2008-12-19")),
        new Episode(1, 3, 6, "Calendar Geeks", date("2008-12-26")),
        new Episode(1, 4, 1, "Jen The Fredo", date("2010-06-25")),
        new Episode(1, 4, 2, "The Final Countdown", date("2010-07-02")),
        new Episode(1, 4, 3, "Something Happened", date("2010-07-09")),
        new Episode(1, 4, 4, "Italian For Beginners", date("2010-07-16")),
        new Episode(1, 4, 5, "Bad Boys", date("2010-07-23")),
        new Episode(1, 4, 6, "Reynholm vs Reynholm", date("2010-07-30")),
        new Episode(2, 1, 1, "Minimum Viable Product", date("2014-04-06")),
        new Episode(2, 1, 2, "The Cap Table", date("2014-04-13")),
        new Episode(2, 1, 3, "Articles of Incorporation", date("2014-04-20")),
        new Episode(2, 1, 4, "Fiduciary Duties", date("2014-04-27")),
        new Episode(2, 1, 5, "Signaling Risk", date("2014-05-04")),
        new Episode(2, 1, 6, "Third Party Insourcing", date("2014-05-11")),
        new Episode(2, 1, 7, "Proof of Concept", date("2014-05-18")),
        new Episode(2, 1, 8, "Optimal Tip-to-Tip Efficiency", date("2014-06-01")),
        new Episode(2, 2, 1, "Sand Hill Shuffle", date("2015-04-12")),
        new Episode(2, 2, 2, "Runaway Devaluation", date("2015-04-19")),
        new Episode(2, 2, 3, "Bad Money", date("2015-04-26")),
        new Episode(2, 2, 4, "The Lady", date("2015-05-03")),
        new Episode(2, 2, 5, "Server Space", date("2015-05-10")),
        new Episode(2, 2, 6, "Homicide", date("2015-05-17")),
        new Episode(2, 2, 7, "Adult Content", date("2015-05-24")),
        new Episode(2, 2, 8, "White Hat/Black Hat", date("2015-05-31")),
        new Episode(2, 2, 9, "Binding Arbitration", date("2015-06-07")),
        new Episode(2, 2, 10, "Two Days of the Condor", date("2015-06-14")),
        new Episode(2, 3, 1, "Founder Friendly", date("2016-04-24")),
        new Episode(2, 3, 2, "Two in the Box", date("2016-05-01")),
        new Episode(2, 3, 3, "Meinertzhagen's Haversack", date("2016-05-08")),
        new Episode(2, 3, 4, "Maleant Data Systems Solutions", date("2016-05-15")),
        new Episode(2, 3, 5, "The Empty Chair", date("2016-05-22")),
        new Episode(2, 3, 6, "Bachmanity Insanity", date("2016-05-29")),
        new Episode(2, 3, 7, "To Build a Better Beta", date("2016-06-05")),
        new Episode(2, 3, 8, "Bachman's Earnings Over-Ride", date("2016-06-12")),
        new Episode(2, 3, 9, "Daily Active Users", date("2016-06-19")),
        new Episode(2, 3, 10, "The Uptick", date("2016-06-26")),
        new Episode(2, 4, 1, "Success Failure", date("2017-04-23")),
        new Episode(2, 4, 2, "Terms of Service", date("2017-04-30")),
        new Episode(2, 4, 3, "Intellectual Property", date("2017-05-07")),
        new Episode(2, 4, 4, "Teambuilding Exercise", date("2017-05-14")),
        new Episode(2, 4, 5, "The Blood Boy", date("2017-05-21")),
        new Episode(2, 4, 6, "Customer Service", date("2017-05-28")),
        new Episode(2, 4, 7, "The Patent Troll", date("2017-06-04")),
        new Episode(2, 4, 8, "The Keenan Vortex", date("2017-06-11")),
        new Episode(2, 4, 9, "Hooli-Con", date("2017-06-18")),
        new Episode(2, 4, 10, "Server Error", date("2017-06-25")),
        new Episode(2, 5, 1, "Grow Fast or Die Slow", date("2018-03-25")),
        new Episode(2, 5, 2, "Reorientation", date("2018-04-01")),
        new Episode(2, 5, 3, "Chief Operating Officer", date("2018-04-08")),
        new Episode(2, 5, 4, "Tech Evangelist", date("2018-04-15")),
        new Episode(2, 5, 5, "Facial Recognition", date("2018-04-22")),
        new Episode(2, 5, 6, "Artificial Emotional Intelligence", date("2018-04-29")),
        new Episode(2, 5, 7, "Initial Coin Offering", date("2018-05-06")),
        new Episode(2, 5, 8, "Fifty-One Percent", date("2018-05-13"))
    );

    private AppData() { }

    private static Date date(String str) {
        return Date.valueOf(LocalDate.parse(str));
    }

    public static class Series {
        private final long seriesID;
        private final String title;
        private final Date releaseDate;
        private final String seriesInfo;

        public Series(long seriesID, String title, Date releaseDate, String seriesInfo) {
            this.seriesID = seriesID;
            this.title = title;
            this.releaseDate = releaseDate;
            this.seriesInfo = seriesInfo;
        }

        public long seriesID() {
            return seriesID;
        }

        public String title() {
            return title;
        }

        public Date releaseDate() {
            return releaseDate;
        }

        public String seriesInfo() {
            return seriesInfo;
        }
    }

    public static class Season {
        private final long seriesID;
        private final long seasonID;
        private final String title;
        private final Date firstAired;
        private final Date lastAired;

        public Season(long seriesID, long seasonID, String title, Date firstAired, Date lastAired) {
            this.seriesID = seriesID;
            this.seasonID = seasonID;
            this.title = title;
            this.firstAired = firstAired;
            this.lastAired = lastAired;
        }

        public long seriesID() {
            return this.seriesID;
        }

        public long seasonID() {
            return this.seasonID;
        }

        public String title() {
            return this.title;
        }

        public Date firstAired() {
            return this.firstAired;
        }

        public Date lastAired() {
            return this.lastAired;
        }
    }

    public static class Episode {
        private final long seriesID;
        private final long seasonID;
        private final long episodeID;
        private final String title;
        private final Date airDate;

        public Episode(long seriesID, long seasonID, long episodeID, String title, Date airDate) {
            this.seriesID = seriesID;
            this.seasonID = seasonID;
            this.episodeID = episodeID;
            this.title = title;
            this.airDate = airDate;
        }

        public long seriesID() {
            return seriesID;
        }

        public long seasonID() {
            return seasonID;
        }

        public long episodeID() {
            return episodeID;
        }

        public String title() {
            return title;
        }

        public Date airDate() {
            return airDate;
        }
    }
}
