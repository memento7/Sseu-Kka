from KINCluster import Item

class myItem(Item):
    def __repr__(self):
        quotes = " ".join(self.quotes)
        return quotes + " " + " ".join([self.title, self.content, self.keyword])

    def __str__(self):
        quotes = " ".join(self.quotes)
        return quotes + " " + " ".join([self.title, self.content, self.keyword])
