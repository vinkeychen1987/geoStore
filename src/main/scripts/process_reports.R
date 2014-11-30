library(iotools)
library(gdata)

system(paste0("hadoop fs -text /projects/locstore/reports/2014/09/03/* > ", tf <- tempfile()))
x = scan(tf, what="character", sep="\t")
m = mstrsplit(x, "|")

types = c("SMSD", "AWSV", "AWSD_3G", "AWSD_4G", "NELOS", "CLOSENUPH", "WIFI", "UNKNOWN")

z = m[m[,3] == "e",c(1,2,4)]
#types = unique(z[,1])
codes = unique(z[,2])
mat = matrix(0, nrow=length(codes), ncol=length(types))
rownames(mat) = codes
colnames(mat) = types
for(i in 1:nrow(z)) {
  mat[match(z[i,2], codes), match(z[i,1], types)] = as.numeric(z[i,3])
}
mat = mat[order(apply(mat, 1, sum), decreasing=TRUE),]
df = as.data.frame(rbind(colnames(mat),matrix(as.character(mat), ncol=ncol(mat))))
rownames(df) = c("", rownames(mat))
write.fwf(df, file="temp", justify="right", colnames=FALSE, rownames=TRUE)

z = m[m[,1] == "g",2:4]
#types = unique(z[,1])
codes = unique(z[,2])
mat = matrix(0, nrow=length(codes), ncol=length(types))
rownames(mat) = codes
colnames(mat) = types
for(i in 1:nrow(z)) {
  mat[match(z[i,2], codes), match(z[i,1], types)] = as.numeric(z[i,3])
}
mat = mat[order(apply(mat, 1, sum), decreasing=TRUE),]






