const Redis = require('ioredis');
const redis = new Redis();
const fs = require('fs');
const csv = require('csv-parser');
const readline = require('readline');
const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
});

const { Movie, GetTokens, intersection } = require("./services");

(async () => {
        const csvFilePath = "db/Top_10000_Movies_IMDb.csv"
        let index = 1
        fs.createReadStream(csvFilePath)
                .pipe(csv())
                .on('data', (row) => {
                        // Process each row of the CSV
                        let { Movie_Name, Genre, Plot, Directors, Stars } = row
                        const movie = new Movie(Movie_Name, Genre, Plot, Directors, Stars)
                        const movieString = JSON.stringify(movie)
                        const tokens = GetTokens(movieString)
                        redis.set(index, Movie_Name);
                        for (let i = 0; i < tokens.length; i++) {
                                redis.rpush(tokens[i], index, (err, reply) => {
                                });
                        }
                        index++

                })
                .on('end', () => {
                        // All rows have been processed
                        rl.question('Enter the movie related search query ((+_+)) : ', (query) => {
                                queryTokens = GetTokens(query);
                                let promises = [];
                                let intersectionArray = [];

                                for (let i = 0; i < queryTokens.length; i++) {
                                        let promise = new Promise((resolve, reject) => {
                                                redis.lrange(queryTokens[i], 0, -1, (err, list) => {
                                                        if (err) {
                                                                reject(err);
                                                        } else {
                                                                resolve(list);
                                                        }
                                                });
                                        });
                                        promises.push(promise);
                                }

                                Promise.all(promises)
                                        .then((lists) => {
                                                for (let i = 0; i < lists.length; i++) {
                                                        console.log(lists[i]);
                                                        if (intersectionArray.length === 0) {
                                                                intersectionArray = lists[i];
                                                        } else {
                                                                intersectionArray = intersection(intersectionArray, lists[i]);
                                                        }
                                                }
                                                let unique = [];
                                                for (i = 0; i < intersectionArray.length; i++) {
                                                        if (unique.indexOf(intersectionArray[i]) === -1) {
                                                                unique.push(intersectionArray[i]);
                                                        }
                                                }
                                                console.log("Movie Names :")
                                                unique.forEach(index => {
                                                        redis.get(index, (err, value) => {
                                                                if (err) {
                                                                        console.error('Error:', err);
                                                                        return;
                                                                }

                                                                console.log(value);
                                                        });
                                                });
                                                rl.close();
                                        })
                                        .catch((err) => {
                                                console.error('Error:', err);
                                                rl.close();
                                        });
                        });


                })
                .on('error', (error) => {
                        console.error('Error processing CSV file:', error);
                });
})();