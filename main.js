const Redis = require('ioredis');
const redis = new Redis(); // Connect to the default Redis server on localhost and default port (6379)
const fs = require('fs');
const csv = require('csv-parser');
const readline = require('readline');
const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
});


function Movie(Movie_Name, Genre, Plot, Directors, Stars) {
        this.Movie_Name = Movie_Name;
        this.Genre = Genre;
        this.Plot = Plot;
        this.Directors = Directors;
        this.Stars = Stars;
}

function tokenize(text) {
        return text.split(/\W+/).filter(token => token.length > 0);
}

function stopwordFilter(tokens) {
        const stopwords = {
                "movie_name": true,
                "genre": true,
                "plot": true,
                "an": true,
                "directors": true,
                "stars": true,
                "a": true,
                "and": true,
                "be": true,
                "have": true,
                "i": true,
                "in": true,
                "of": true,
                "that": true,
                "the": true,
                "to": true
        };
        const result = [];
        for (let token of tokens) {
                if (!stopwords[token]) {
                        result.push(token);
                }
        }
        return result;
}
function GetTokens(text) {
        let tokens = tokenize(text);
        tokens = lowercaseFilter(tokens);
        tokens = stopwordFilter(tokens)
        return tokens;
}

function lowercaseFilter(tokens) {
        return tokens.map(token => token.toLowerCase());
}

function intersection(a, b) {
        const maxLen = Math.max(a.length, b.length);
        const r = new Set();
        let i = 0, j = 0;
        while (i < a.length && j < b.length) {
                if (a[i] < b[j]) {
                        i++;
                } else if (a[i] > b[j]) {
                        j++;
                } else {
                        r.add(a[i]);
                        i++;
                        j++;
                }
        }
        return r;
}


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
                                        // if (err) {
                                        //         console.error('Error:', err);
                                        //         return;
                                        // }
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
                                                        if (intersectionArray.length === 0) {
                                                                intersectionArray = lists[i];
                                                        } else {
                                                                intersectionArray = intersection(intersectionArray, lists[i]);
                                                        }
                                                }
                                                console.log(intersectionArray)
                                                console.log("Movie Names :")
                                                intersectionArray.forEach(index => {
                                                        redis.get(index, (err, value) => {
                                                                if (err) {
                                                                        console.error('Error:', err);
                                                                        return;
                                                                }

                                                                console.log(value);
                                                                redis.quit();
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
                        // Handle error while reading/parsing CSV
                        console.error('Error processing CSV file:', error);
                });
})();