//0. Rating of size 2
db.anime.find({ ratings: { $size: 2 } })

//1. Find anime with a rating of "Rx - Hentai" and more than 5 favorites.
db.anime.find({ rating: "Rx - Hentai", favorites: { $gt: 5 } })

//2. Retrieve anime with more than 1000 completed episodes.
db.anime.find({ "watchStatistic.completed": { $gt: 1000 } })

//3.Find anime with the genre "Horror" and a score higher than 8.
db.anime.find({ genres: { $in: [/Horror/] }, score: { $gt: 8 } })

//4.Fetch anime produced by "Discovery" with a duration of less than 40 minutes.
db.anime.find({ producers: { $in: [/Discovery/] }, duration: { $lt: 40 } })

//5.Get anime with a popularity score above 8000 and at least one rating with a score of 9 or 10.
db.anime.find({ popularity: { $gt: 8000 }, "ratings.rating": { $in: [9, 10] } })

//6. Find anime with more than 2000 members and fewer than 5 episodes.
db.anime.find({ members: { $gt: 2000 }, episodes: { $lt: 5 } })

//7. Retrieve anime with a median score (around 5.76).
db.anime.find({ score: { $gte: 5.75, $lte: 5.77 } })

//8. Find anime with at least one user rating and display the user ID, watched episodes, and rating.
db.anime.aggregate([
    { $unwind: "$ratings" },
    { $project: { "ratings.userId": 1, "ratings.watchedEpisodes": 1, "ratings.rating": 1 } }
])

//9. Identify the most common genre among anime with a score higher than 8.
db.anime.aggregate([
    { $match: { score: { $gt: 8.00 } } },
    { $unwind: "$genres" },
    { $group: { _id: "$genres", cunt: { $count: {} } } },
    { $sort: { cunt: -1 } }
])

//10. Get anime with more than 100 episodes that are either completed or currently being watched.
db.anime.find({ episodes: { $gt: 100 }, $or: [{ "watchStatistic.watching": { $gt: 100 } }, { "watchStatistic.completed": { $gt: 100 } }] })

//1. Calculate the average number of watched episodes for each genre using the aggregation framework.
db.anime.aggregate([
    { $unwind: "$genres" },
    { $unwind: "$ratings" },
    { $group: { _id: "$genres", average: { $avg: "$ratings.watchedEpisodes" } } }
])

//2. Determine the top anime producers based on the average score of the anime they produced using map-reduce.
var map = function() {
    if (this.producers != null)
        this.producers.forEach(a => {
            emit(a, this.score)
        })
}

var reduce = function(key, values) {
    return Array.avg(values)
}

db.anime.mapReduce(map, reduce, { out: "producersByAverageAnimeScore" }).find({}).sort({ value: -1 })

//3. Use aggregation to find the distribution of watch statuses (completed, watching, on-hold, etc.) across all anime.
db.anime.aggregate([
    { $project: { watchStatus: { $objectToArray: "$watchStatistic" } } },
    { $unwind: "$watchStatus" },
    { $group: { _id: "$watchStatus.k", count: { $sum: "$watchStatus.v" } } },
    { $sort: { count: -1 } }
])

//4. Use map-reduce to analyze how the popularity of genres has changed over time.
var map = function() {
    if (this.genres != null)
        this.genres.forEach(a => {
            emit(a, this.popularity)
        })
}

var reduce = function(key, values) {
    return Array.sum(values)
}

db.anime.mapReduce(map, reduce, { out: "genrePopularity" }).find({}).sort({ value: -1 })
db.anime.mapReduce(
    function() {
        if (this.genres != null)
            this.genres.forEach(a => {
                emit(a, this.popularity)
            })
    },
    function(key, values) {
        return Array.sum(values);
    }, 
    { out: "genrePopularity" }
).find({}).sort({ value: -1 })

//5. Aggregate the total number of reviews and the average rating given by each user.
db.anime.aggregate([
    {$unwind: "$ratings"},
    {$group: { _id: "$ratings.userId", total: {$count: {}}, average: {$avg: "$ratings.rating"}}},
    {$sort: {average: -1}}
])

db.anime.mapReduce(
    function() {
        if (this.ratings != null)
        this.ratings.forEach(a => {
            emit(a.userId, {count: 1, sum: a.rating})
        })
    },
    function(key, values) {
        var count = 0, sum = 0
        values.forEach(a => {
            count += a.count
            sum += a.sum
        })
        return {_id: key, count: count, avg: (sum/values.length)}
    },
    {
        out: "averageRatingByUser"
    }
)

//6. Calculate the distribution of scores for each type of anime (TV, OVA, Movie) using map-reduce.
db.anime.mapReduce(
    function() {
        if(this.score != null)
        emit(this.type, {count: 1, score: this.score})
    },
    function(key, values) {
        var result = {count: 0, totalScore: 0, avgScore: 0}
        values.forEach(a => {
            result.count += a.count
            result.totalScore += a.score
        })
        result.avgScore = result.totalScore / result.count
        return result
    },
    { out: "scoresByType"}
)

//7. Find the top genres based on the total number of members who have watched anime in those genres.
db.anime.mapReduce(
    function() {
        if(this.genres != null)
        this.genres.forEach(a => {
            emit(a, this.members)
        })
    },
    function(key, values) {
        return Array.sum(values)
    },
    { out: "genresByMembers"}
)

db.genresByMembers.find({}).sort({ value:-1 })

//8. Aggregate the average number of episodes and the average score for anime with a similar number of episodes.
db.anime.aggregate([
    {$group: { _id: "$episodes", avgScore: {$avg: "$score"}, count: {$count: {}}}},
    {$match: {count: {$gt: 10}}},
    {$sort: {avgScore: -1}}
])

//9. Use map-reduce to identify the top users based on the total number of watched episodes and the average rating given.
db.anime.mapReduce(
    function() {
        if(this.ratings != null)
        this.ratings.forEach(a => {
            emit(a.userId, {watchedEpisodes: a.watchedEpisodes, rating: a.rating})
        })
    },
    function(key, values) {
        var watched = 0, rating = 0
        values.forEach(a => {
            watched += a.watchedEpisodes
            rating += a.rating
        })
        return {totalWatched: watched, avgRating: (rating/values.length)}
    },
    {out : "usersByWatchedAndRating"}
)

db.usersByWatchedAndRating.find({}).sort({ "value.totalWatched":-1 })

// ----------[UPIT SA ODBRANE]----------
// Koja rec se najvise pojavljuje u synopsis-u
db.anime.aggregate([
    {$project: {reci: {$split: ["$synopsis", " "]}}},
    {$unwind: "$reci"},
    {$group: { _id: "$reci", broj: {$count: {}}}},
    {$sort: {broj: -1}},
    {$skip: 40},
    {$limit: 20}
])