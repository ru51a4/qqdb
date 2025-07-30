let counter = 0;
for (let i = 0; i <= a.length - 1; i++) {
    let min = a[i]
    let x = i
    for (let j = i; j <= a.length - 1; j++) {
        if (min > a[j]) {
            min = a[j];
            x = j;
        }
    }
    console.log(min)
    if (x !== i) {
        let t = a[i]
        a[i] = a[x];
        a[x] = t;
        counter++;
        counter++;
    }
}