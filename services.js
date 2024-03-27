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
        const r = [];
        let i = 0, j = 0;

        while (i < a.length && j < b.length) {
                if (a[i] < b[j]) {
                        i++;
                } else if (a[i] > b[j]) {
                        j++;
                } else {
                        r.push(a[i]);
                        i++;
                        j++;
                }
        }

        return r;
}

module.exports = {
        Movie,
        GetTokens,
        intersection
};      