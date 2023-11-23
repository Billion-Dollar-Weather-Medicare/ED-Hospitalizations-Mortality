library(choroplethr) 
library(choroplethrMaps)
library(ggplot2) 
library(RColorBrewer)
library(reshape2)
library(cowplot)
library(pals)

data(county.map)
all_counties <- unique(county.map$region) #all counties from r map data
all_counties<- sort(all_counties)
allcounties <- data.frame(all_counties)
colnames(allcounties) <- c("region")

affected <- read.csv("NEW_num_affected_no_drt_wf.csv", stringsAsFactors = FALSE)
affected_keep <- affected[,c("County_FIPS", "event_cat")]
colnames(affected_keep) <- c("region","value")

all_counties_affected <- merge(allcounties, affected_keep, by="region", all.x=TRUE)


all_counties_affected[is.na(all_counties_affected)] <- "0"
all_counties_affected[all_counties_affected$value=="5",]$value <- "5+"
all_counties_affected$value <- as.factor(all_counties_affected$value)

# MAP
colorscale_init <- brewer.blues(9)
colorscale_init[1] <- "#FFFFFF"
colorscale <- colorscale_init[c(1,3,5, 6,8,9)]
choro_affected <- CountyChoropleth$new(all_counties_affected)
choro_affected$ggplot_scale <- scale_fill_manual(name="Number of Events", values=colorscale, drop=FALSE, theme_bw())
choro_affected$render() + theme(legend.key.size = unit(.5, 'cm'), #change legend key size
                                legend.key.height = unit(.5, 'cm'), #change legend key height
                                legend.key.width = unit(.5, 'cm'), #change legend key width
                                legend.title = element_text(size=8, family="sans"), #change legend title font size
                                legend.text = element_text(size=8, family="sans"),
                                #legend.title.align = -1.5,
                                #legend.box.spacing = unit(-1, 'cm'),
                                legend.box.margin=margin(10,10,10,-20),
                                legend.margin = margin(0,0,0,0)
)
# save as PDF with 10 x 12 dimensions (landscape) 