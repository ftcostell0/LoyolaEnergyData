setwd("~/GitHub/LoyolaEnergyData")
library(data.table)
library(dplyr)

df <- fread('output.csv')

df <- df %>%
  filter(df$Type == "Electric Usage", !is.na(df$Temperature)) %>%
  data.frame()


model <- glm(Usage ~ Occupancy + Temperature, data = df)
summary(model)


plot(model)

plot(df$Temperature,df$Usage)
